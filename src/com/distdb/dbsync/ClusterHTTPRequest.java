package com.distdb.dbsync;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.HttpHelpers.HeaderDecoder;
import com.distdb.HttpHelpers.HelperJson;
import com.distdb.dbserver.Cluster;
import com.distdb.dbserver.DBObject;
import com.distdb.dbserver.Database;
import com.distdb.dbserver.DistServer.DBType;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ClusterHTTPRequest implements Runnable {

	private static final String newLine = "\r\n";
	private Logger log;
	private Socket socket;
	private Cluster cluster;
	private Map<String, Database> dbs;

	private Map<String, String> headerFields;
	private String body;

	public ClusterHTTPRequest(Logger log, Socket s, Cluster cluster, Map<String, Database> dbs) {
		this.log = log;
		this.socket = s;
		this.cluster = cluster;
		this.dbs = dbs;
		headerFields = new HashMap<>();
		body = null;
	}

	@Override
	public void run() {
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			OutputStream out = new BufferedOutputStream(socket.getOutputStream());
			PrintStream pout = new PrintStream(out);

			// read first line of request
			String request = in.readLine();
			if (request == null || request.length() == 0)
				return;

			// Rest of Header
			while (true) {
				String header = in.readLine();
				if (header == null || header.length() == 0)
					break;
				headerFields.put(header.split(":", 2)[0].trim(), header.split(":", 2)[1].trim());
			}

			// Body, if any
			if (headerFields.get("Content-Length") != null
					&& Integer.parseInt(headerFields.get("Content-Length")) > 0) {
				StringBuilder sb = new StringBuilder();
				char[] cb = new char[Integer.parseInt(headerFields.get("Content-Length"))];
				int bytesread = in.read(cb, 0, Integer.parseInt(headerFields.get("Content-Length")));
				sb.append(cb, 0, bytesread);
				body = sb.toString();
			}

			HeaderDecoder reqLine = new HeaderDecoder(request);

			// log.log(Level.INFO, "Serving {0}",
			// reqLine.command + " " + reqLine.resource + " from " +
			// socket.getInetAddress().toString());

			String response = "";

			// Only POSTs are allowed
			boolean reqValid = body != null && reqLine.command.equals("POST") && body.length() != 0
					&& (reqLine.protocol.equals("HTTP/1.0") || reqLine.protocol.equals("HTTP/1.1"));

			if (reqValid)
				response = processPost(reqLine, body);
			else// Bad Request
				response = "HTTP/1.0 400 Bad Request" + newLine + newLine;

			pout.print(response);
			pout.close();
			out.close();
			in.close();
			socket.close();

		} catch (IOException e) {
			log.log(Level.WARNING, "Cannot read from socket");
			log.log(Level.WARNING, Arrays.toString(e.getStackTrace()));
		}

	}

	public String processPost(HeaderDecoder reqLine, String body) {
		String[] ret = new String[2];
		String resp = "";

		String[] res = reqLine.resource.split("/"); // First element should be DataBase name, Second must be the command

		if (res == null || res.length < 2 || res.length > 3) {
			resp = "HTTP/1.1 400 Bad Request (Expected databaseName/Operation)" + newLine + newLine;

		} else if (res.length == 2) {
			if (res[1].equals("ping")) {
				ret = answerPing(body);
				resp = "HTTP/1.1 200 OK" + newLine + "Content-Type: " + ret[0] + newLine + "Date: " + new Date()
						+ newLine + "Content-length: " + ret[1].length() + newLine + newLine + ret[1];
			} else if (res[1].equals("joinCluster")) {
				ret = joinCluster(body);
				resp = "HTTP/1.1 200 OK" + newLine + "Content-Type: " + ret[0] + newLine + "Date: " + new Date()
						+ newLine + "Content-length: " + ret[1].length() + newLine + newLine + ret[1];
			} else if (res[1].equals("leaveCluster")) {
				ret = leaveCluster(body);
				resp = "HTTP/1.1 200 OK" + newLine + "Content-Type: " + ret[0] + newLine + "Date: " + new Date()
						+ newLine + "Content-length: " + ret[1].length() + newLine + newLine + ret[1];
			} else
				resp = "HTTP/1.1 400 Bad Request (Unknown Operation)" + newLine + newLine;

		} else if (res.length == 3) {
			String dbname = res[1];
			String operation = res[2];
			if (operation.equals("getObjectFile")) { // From Replica: Master must send datafile
				ret = sendObjectFile(dbname, body);
				resp = "HTTP/1.1 200 OK" + newLine + "Content-Type: " + ret[0] + newLine + "Date: " + new Date()
						+ newLine + "Content-length: " + ret[1].length() + newLine + newLine + ret[1];
			} else if (operation.equals("getLoggingFile")) { // From Replica: Master must send logfile
				ret = sendLoggingFile(dbname, body);
				resp = "HTTP/1.1 200 OK" + newLine + "Content-Type: " + ret[0] + newLine + "Date: " + new Date()
						+ newLine + "Content-length: " + ret[1].length() + newLine + newLine + ret[1];
			} else if (operation.equals("sendUpdate")) { // From Master. Replica must accept logging operations
				ret = getUpdate(dbname, body);
				resp = "HTTP/1.1 200 OK" + newLine + "Content-Type: " + ret[0] + newLine + "Date: " + new Date()
						+ newLine + "Content-length: " + ret[1].length() + newLine + newLine + ret[1];
			} else {
				resp = "HTTP/1.1 400 Bad Request (Unknown Operation)" + newLine + newLine;
			}

		} else
			resp = "HTTP/1.1 400 Bad Request (Unknown Operation)" + newLine + newLine;

		return resp;
	}

	private String[] answerPing(String body) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		JsonElement je = new JsonParser().parse(body);
		if (je.isJsonObject()) {
			JsonObject jo = je.getAsJsonObject();
			String user = jo.get("user").getAsString();
			String token = jo.get("token").getAsString();
			if (true) { // Reserved for authentication
				ret[1] = HelperJson.returnCodes("OK", cluster.myURL.toString(), "");
			}
		} else {
			ret[1] = HelperJson.returnCodes("FAIL", "Invalid Json command", "");
		}
		return ret;
	}

	private String[] joinCluster(String body) {
		String[] ret = new String[2];
		ret[0] = "application/json";

		if (cluster.myType == DBType.REPLICA) {
			ret[1] = HelperJson.returnCodes("FAIL", "I'm a replica. Another replica tries to join me", "");
			return ret;
		}

		JsonElement je = new JsonParser().parse(body);
		if (je.isJsonObject()) {
			JsonObject jo = je.getAsJsonObject();
			String user = jo.get("user").getAsString();
			String token = jo.get("token").getAsString();
			if (user.equals(jo.get("user").getAsString())) { // True Reserved for authentication
				String replicaName = jo.get("replicaName").getAsString();
				String[] temp = new String[2];
				temp = cluster.replicaWantsToJoinCluster(replicaName);
				if (temp[0].equals("OK"))
					ret[1] = HelperJson.returnCodes("OK", replicaName + " has joined the cluster", "");
				else
					ret[1] = HelperJson.returnCodes("FAIL", temp[1], "");
			}
		}
		return ret;
	}
	
	private String[] leaveCluster(String body) {
		String[] ret = new String[2];
		ret[0] = "application/json";

		if (cluster.myType == DBType.REPLICA) {
			ret[1] = HelperJson.returnCodes("FAIL", "I'm a replica and cannot attend this request", "");
			return ret;
		}

		JsonElement je = new JsonParser().parse(body);
		if (je.isJsonObject()) {
			JsonObject jo = je.getAsJsonObject();
			String user = jo.get("user").getAsString();
			String token = jo.get("token").getAsString();
			if (user.equals(jo.get("user").getAsString())) { // True Reserved for authentication
				ret = cluster.replicaWantsToLeaveCluster(jo.get("replicaName").getAsString());
			} else {
				ret[1] = HelperJson.returnCodes("FAIL", "Unauthorized", "");
			}
		} else
			ret[1] = HelperJson.returnCodes("FAIL", "Malformed Json request", "");
		
		return ret;
	}

	private String[] sendObjectFile(String dbName, String body) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		JsonElement je = new JsonParser().parse(body);
		if (cluster.myType == DBType.REPLICA) {
			ret[1] = HelperJson.returnCodes("FAIL", "Replicas cannot send object collections", "");
			return ret;
		}
		if (je.isJsonObject()) {
			JsonObject jo = je.getAsJsonObject();
			String user = jo.get("user").getAsString();
			String token = jo.get("token").getAsString();
			if (user.equals(jo.get("user").getAsString())) { // True Reserved for authentication
				String objectName = jo.get("objectName").getAsString();
				DBObject dbo = null;
				if (dbs.get(dbName) != null)
					dbo = dbs.get(dbName).dbobjs.get(objectName);
				if (dbo == null) {
					ret[1] = HelperJson.returnCodes("FAIL",
							"Invalid database name (" + dbName + ") or object collection name (" + objectName + ")",
							"");
					return ret;
				}
				// Send the collection of the requested objects

				String dataFile = dbs.get(dbName).getMasterInfo()[0] + "/_data_" + objectName;
				String content = "";
				try {
					content = new String(Files.readAllBytes(Paths.get(dataFile)));
					JsonElement je2 = new JsonParser().parse(content);
					ret[1] = HelperJson.returnCodes("OK", objectName + " datafile sent",
							je2.getAsJsonObject().toString());
				} catch (IOException e) {
					log.log(Level.SEVERE, "cannot read & send log file for " + dbName);
					log.log(Level.SEVERE, e.getMessage());
					log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
					ret[1] = HelperJson.returnCodes("FAIL", "No file", content);
				}
			} else {
				ret[1] = HelperJson.returnCodes("FAIL", "Not Authorized", "");
			}
		} else {
			ret[1] = HelperJson.returnCodes("FAIL", "Invalid Json command", "");
		}
		return ret;
	}

	private String[] sendLoggingFile(String dbName, String body) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		JsonElement je = new JsonParser().parse(body);
		if (cluster.myType == DBType.REPLICA) {
			ret[1] = HelperJson.returnCodes("FAIL", "Replicas cannot send Logging files", "");
			return ret;
		}
		if (je.isJsonObject()) {
			JsonObject jo = je.getAsJsonObject();
			String user = jo.get("user").getAsString();
			String token = jo.get("token").getAsString();
			if (user.equals(jo.get("user").getAsString())) { // True by now. Reserved for authentication

				// Send the Logging file for the requested database

				String loggingFile = dbs.get(dbName).getMasterInfo()[0] + dbName + "_logging";
				String content = "";
				try {
					content = new String(Files.readAllBytes(Paths.get(loggingFile)));
					ret[1] = HelperJson.returnCodes("OK", dbName + " Log file sent", content);
				} catch (IOException e) {
					// log.log(Level.SEVERE, "cannot read & send log file for " + dbName);
					// log.log(Level.SEVERE, e.getMessage());
					// log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
					System.err.println("Cannot read & send log file for " + dbName);
					ret[1] = HelperJson.returnCodes("FAIL", "No file", "");
				}
			} else {
				ret[1] = HelperJson.returnCodes("FAIL", "Nor Authorized", "");
			}
		} else {
			ret[1] = HelperJson.returnCodes("FAIL", "Invalid Json command", "");
		}
		return ret;
	}

	private String[] getUpdate(String dbName, String body) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		JsonElement je = new JsonParser().parse(body);
		if (cluster.myType == DBType.MASTER) {
			ret[1] = HelperJson.returnCodes("FAIL", "A master should not receive this request", "");
			return ret;
		}

		if (je.isJsonObject()) {
			JsonObject jo = je.getAsJsonObject();
			String user = jo.get("user").getAsString();
			String token = jo.get("token").getAsString();
			if (user.equals(jo.get("user").getAsString())) { // True by now. Reserved for authentication

				// This replica has got a new logging that must update database
			}
		}

		return ret;
	}
}
