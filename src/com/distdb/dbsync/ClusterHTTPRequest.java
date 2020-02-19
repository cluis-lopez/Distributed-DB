package com.distdb.dbsync;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbserver.Cluster;
import com.distdb.dbserver.MasterDatabase;

public class ClusterHTTPRequest implements Runnable {

	private static final String newLine = "\r\n";
	private Logger log;
	private Socket socket;
	private Cluster cluster;

	private Map<String, String> headerFields;
	private String body;

	public ClusterHTTPRequest(Logger log, Socket s, Cluster cluster) {
		this.log = log;
		this.socket = s;
		this.cluster = cluster;
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
			if (reqValid && reqLine.command.equals("POST"))
				response = processPost(reqLine);
			if (!reqValid) // Bad Request
				response = "HTTP/1.0 400 Bad Request" + newLine + newLine;

			pout.print(response);
			pout.close();
			out.close();
			in.close();

		} catch (IOException e) {
			log.log(Level.WARNING, "Cannot read from socket");
			log.log(Level.WARNING, Arrays.toString(e.getStackTrace()));
		}

	}
	public String processGet(HeaderDecoder reqLine) {
		String resp = "";
		return resp;
	}
	
	public String processPost(HeaderDecoder  reqLine) {
		String resp = "";
		return resp;
	}
}
