package com.distdb.HTTPDataserver;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbserver.Database;
import com.distdb.dbserver.MasterDatabase;
import com.distdb.dbsync.DiskSyncer;

public class DataServerAPI implements Runnable {

	private static final String newLine = "\r\n";
	private Logger log;
	private String adminRootPath;
	private Map<String, Database> dbs;
	private Socket socket;
	private DiskSyncer dSync;

	private Map<String, String> headerFields;
	private String body;

	public DataServerAPI(Logger log, Socket s, String adminRootPath, Map<String, Database> dbs, DiskSyncer dSync) {
		this.log = log;
		this.adminRootPath = adminRootPath;
		this.dbs = dbs;
		this.socket = s;
		this.dSync = dSync;
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
			// Logging

			log.log(Level.INFO, "Serving {0}",
					reqLine.command + " " + reqLine.resource + " from " + socket.getInetAddress().toString());

			String response = "";

			boolean reqValid = (reqLine.command.equals("GET") || reqLine.command.equals("POST"))
					&& (reqLine.protocol.equals("HTTP/1.0") || reqLine.protocol.equals("HTTP/1.1"));

			if (reqValid && reqLine.command.equals("GET"))
				response = processGet(reqLine);
			// HTTP POST only accepts Json payloads
			if (reqValid && reqLine.command.equals("POST") && headerFields.get("Content-Type") != null
					&& headerFields.get("Content-Type").equals("application/json"))
				response = processPost(reqLine);
			else
				response = "HTTP/1.1 400 Bad Request (Json payload required)" + newLine + newLine;

			if (!reqValid) // Bad Request
				response = "HTTP/1.1 400 Bad Request" + newLine + newLine;

			System.out.println(response);
			pout.print(response);
			pout.flush();
			pout.close();
			out.close();
			in.close();

		} catch (IOException e) {
			log.log(Level.WARNING, "Cannot read from socket");
			log.log(Level.WARNING, Arrays.toString(e.getStackTrace()));
		}
	}

	private String processPost(HeaderDecoder req) {
		String[] ret = new String[2];
		String resp = "";
		Object ob = null;
		String[] res = req.resource.split("/"); // First element should be DataBase name, Second must be the command
		if (res == null || res.length != 3) {
			resp = "HTTP/1.1 400 Bad Request (Expected databaseName/Operation)" + newLine + newLine;
		} else {
			try {
				Class<?> cl = Class.forName("com.distdb.HTTPDataserver.app." + res[2]);
				Constructor<?> cons = cl.getConstructor();
				ob = cons.newInstance(null);
				ob.getClass().getMethod("initialize", Logger.class).invoke(ob, log);
				ret = (String[]) ob.getClass().getMethod("doPost", Map.class, String.class, String.class, DiskSyncer.class).invoke(ob,
						dbs, res[1], body, dSync);
				resp = "HTTP/1.1 200 OK" + newLine + "Content-Type: " + ret[0] + newLine + "Date: " + new Date() + newLine
						+ "Content-length: " + ret[1].length() + newLine + newLine + ret[1];
			} catch (ClassNotFoundException e) {
				log.log(Level.INFO, "Invalid command");
				log.log(Level.INFO, e.getMessage());
				resp = "HTTP/1.1 400 Bad Request Invalid Command" + newLine + newLine;
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | NoSuchMethodException
					| SecurityException e) {
				log.log(Level.SEVERE, "Cannot instantiate command/servlet");
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
				resp = "HTTP/1.1 400 Bad Request Cannot instantiate command/servlet" + newLine + newLine;
			} catch (InvocationTargetException e) {
				log.log(Level.SEVERE, "Invocation Target Exception (problema al instanciar el servlet) ");
				log.log(Level.SEVERE, e.getMessage());
				log.log(Level.SEVERE, Arrays.toString(e.getCause().getStackTrace()));
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
				resp = "HTTP/1.1 400 Bad Request Servlet Inner Exception" + newLine + newLine;
			}
		}
		return resp;
	}

	// HTTP Get should be reserved for static files
	private String processGet(HeaderDecoder req) {
		String[] ret = new String[2];
		String resp;
		
			ret = new doFile(log, adminRootPath).doGet(req.resource.substring(1));
			if (ret[0].equals("")) { // No file found
				resp = "HTTP/1.0 404 " + ret[1] + newLine + newLine;
			} else {
				resp = "HTTP/1.0 200 OK" + newLine + "Content-Type: " + ret[0] + newLine + "Date: " + new Date()
						+ newLine + "Content-length: " + ret[1].length() + newLine + newLine + ret[1];
			}
			return resp;
	}

}
