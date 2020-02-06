package com.distdb.dbserver;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.distdb.HTTPserver.DataServerAPI;
import com.distdb.dbsync.DiskSyncer;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class DistServer {

	static Logger log = Logger.getLogger("DistServer");

	public enum DBType {
		MASTER, REPLICA
	}

	static DiskSyncer dsync;
	static List<Database> dbs;
	static boolean keepRunning = true;
	
	public void kill() {
		DistServer.keepRunning = false;
	}


	public static void main(String[] args) {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT %4$s %5$s%6$s%n");
		FileHandler fd = null;

		try {
			fd = new FileHandler("etc/logs/DistDB.log", true);
		} catch (SecurityException | IOException e1) {
			System.err.println("No se puede abrir el fichero de log");
			e1.printStackTrace();
		}

		// log.setUseParentHandlers(false); // To avoid console logging
		log.addHandler(fd);
		SimpleFormatter formatter = new SimpleFormatter();
		fd.setFormatter(formatter);
		fd.close();

		String propsFile = System.getProperty("ConfigFile");
		if (propsFile == null || propsFile.equals("")) {
			propsFile = "etc/config/DistDB.json";
		}
		Gson json = new Gson();
		Properties props = null;
		try {
			props = json.fromJson(new FileReader(propsFile), Properties.class);
		} catch (JsonSyntaxException | JsonIOException | FileNotFoundException e) {
			System.err.println("Cannot read or parse config file. Check Env variable or etc/DistDB.json file");
			log.log(Level.SEVERE, "Cannot open or access configuration file ... exiting");
			log.log(Level.SEVERE, e.getMessage());
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
		}

		DBType serverType;
		dsync = null;

		if (props.ThisNode.equals("Master") || props.ThisNode.equals("MASTER")) {
			serverType = DBType.MASTER;
			dsync = new DiskSyncer(log, 1000 * props.syncTime);
		} else
			serverType = DBType.REPLICA;
		
		dbs = new ArrayList<>();

		for (Map<String, String> m : props.databases) {
			System.err.println("Inicializando Database: " + m.get("Name"));
			dbs.add(new Database(log, m.get("name"), m.get("defFile"), m.get("defPath"), dsync, serverType));
		}
		

		Thread dSync = new Thread(dsync);
		dSync.start();

		// Arrancamos el servidor de datos

		ServerSocket server = null;

		try {
			server = new ServerSocket(props.dataPort);
		} catch (IOException e) {
			log.log(Level.SEVERE, "Cannot start Server Socket at port: " + props.dataPort);
			log.log(Level.SEVERE, e.getMessage());
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
			System.err.println("No se puede arrancar el server en el puerto " + props.dataPort);
			e.printStackTrace();
		}

		System.out.println("Arrancando el servidor");
		log.log(Level.INFO, "Server started");
		log.log(Level.INFO, "Listening at port: " + props.dataPort);

		Socket client = null;
		while (keepRunning) {
			try {
				client = server.accept();
			} catch (IOException e) {
				log.log(Level.WARNING, "Cannot launch thread to accept client: " + client.toString());
				log.log(Level.WARNING, Arrays.toString(e.getStackTrace()));
			}
			final DataServerAPI request = new DataServerAPI(log, client, dbs);
			Thread thread = new Thread(request);
			thread.start();
		}
	}

	private class Properties {
		String ThisNode;
		int dataPort;
		int clusterPort;
		int syncTime;
		List<Map<String, String>> nodes;
		List<Map<String, String>> databases;
	}
}
