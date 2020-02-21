package com.distdb.dbserver;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.distdb.HTTPDataserver.DataServerAPI;
import com.distdb.dbsync.MasterSyncer;
import com.distdb.dbsync.ClusterHTTPServer;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class DistServer {

	static Logger log = Logger.getLogger("DistServer");

	public enum DBType {
		MASTER, REPLICA
	}

	static Map<String, Database> dbs;
	static EnumMap<DBType, List<Node>> nodes;
	static boolean keepRunning = true;

	public void kill() {
		DistServer.keepRunning = false;
	}

	public static void main(String[] args) {
		new DistServer();
	}

	public DistServer() {
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

		// Initialize Properties

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
			return;
		}

		// Initialize cluster

		DBType type;
		if (props.ThisNode.equals("Master") || props.ThisNode.equals("MASTER"))
			type = DBType.MASTER;
		else
			type = DBType.REPLICA;

		Cluster cluster = new Cluster(log, props.nodes, type);

		if (!cluster.setMaster()) {
			System.err.println("Cannot set cluster Master. Exiting...");
			log.log(Level.SEVERE, "Cannot set cluster Master. Exiting.");
			return;
		}

		String[] temp = cluster.setReplicas();
		if (temp[0].equals("FAIL")) {
			System.err.println("Failing setup cluster " + temp[1]);
			return;
		}

		System.err.println("This node acts as " + type);
		if (type == DBType.MASTER) {
			System.err.println(temp[1]);
			if (cluster.liveReplicas.size() > 0) {
				System.err.println("Replicas living a this time:");
				for (Node n : cluster.liveReplicas)
					System.err.println(n.name);
			}
		}
		
		// If I'm a replica. Joins the cluster
		
		if (type == DBType.REPLICA)
			cluster.joinMeToCluster();

		// Initialize Databases
		MasterSyncer dsync = null;
		ClusterHTTPServer clusterHTTPserver = null;

		if (type == DBType.MASTER)
			dsync = new MasterSyncer(log, cluster, 1000 * props.syncDiskTime, 1000 * props.syncNetTime);

		// Start the cluster HTTP Server
		clusterHTTPserver = new ClusterHTTPServer(props.clusterPort, cluster, dbs);

		dbs = new HashMap<>();

		for (Map<String, String> m : props.databases) {
			System.err.println("Inicializando Database: " + m.get("name"));
			if (type == DBType.MASTER) {
				dbs.put(m.get("name"),
						new MasterDatabase(log, m.get("name"), m.get("defFile"), m.get("defPath"), dsync));
			} else {
				dbs.put(m.get("name"), new ReplicaDatabase(log, m.get("name"), m.get("defFile"), m.get("defPath"),
						cluster.myMaster.url));
			}
			if (m.get("autoStart") != null && (m.get("autoStart").equals("y") || m.get("autoStart").equals("Y")))
				dbs.get(m.get("name")).open();
		}

		// Start Sync Server

		if (type == DBType.MASTER && dsync != null) {
			Thread dSync = new Thread(dsync);
			dSync.setName("Master Syncer");
			dSync.start();
		}

		// Start the cluster server

		Thread clusterServer = new Thread(clusterHTTPserver);
		clusterServer.setName("Net Syncer");

		// Start the Data Server

		ServerSocket server = null;

		try {
			server = new ServerSocket(props.dataPort);
		} catch (IOException e) {
			log.log(Level.SEVERE, "Cannot start Server Socket at port: " + props.dataPort);
			log.log(Level.SEVERE, e.getMessage());
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
			System.err.println("No se puede arrancar el server en el puerto " + props.dataPort);
			e.printStackTrace();
			return;
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
			final DataServerAPI request = new DataServerAPI(log, client, props.adminRootPath, dbs, dsync);
			Thread thread = new Thread(request);
			thread.setName("Request Dispatcher #" + thread.getId());
			thread.start();
			;
		}
	}

	private class Properties {
		String ThisNode;
		int dataPort;
		int clusterPort;
		int syncDiskTime;
		int syncNetTime;
		String adminRootPath;
		List<Map<String, String>> nodes;
		List<Map<String, String>> databases;
	}
}
