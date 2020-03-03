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
import java.util.concurrent.atomic.AtomicBoolean;
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

	private static final Logger log = Logger.getLogger("DistServer");

	public enum DBType {
		MASTER, REPLICA
	}

	static Map<String, Database> dbs;
	static EnumMap<DBType, List<Node>> nodes;
	private static final AtomicBoolean keepRunning = new AtomicBoolean(true);

	public void kill() {
		keepRunning.set(false);
	}

	public static void main(String[] args) {
		new DistServer();
	}

	public DistServer() {

		// Initialize Properties

		String propsFile = System.getenv("ConfigFile");
		if (propsFile == null || propsFile.equals("")) {
			propsFile = "etc/config/DistDB.json";
		}

		System.err.println("Using config file at " + propsFile);

		Gson json = new Gson();
		Properties props = null;
		try {
			props = json.fromJson(new FileReader(propsFile), Properties.class);
		} catch (JsonSyntaxException | JsonIOException | FileNotFoundException e) {
			System.err.println("Cannot read or parse config file. Check Env variable or etc/DistDB.json file");
			System.err.println(e.getMessage());
			System.err.println(Arrays.toString(e.getStackTrace()));
			return;
		}

		System.setProperty("java.util.logging.SimpleFormatter.format",
				props.nodeName + " : " + "%1$tF %1$tT %4$s %5$s%6$s%n");
		FileHandler fd = null;

		try {
			fd = new FileHandler("etc/logs/DistDB.log", true);
		} catch (SecurityException | IOException e1) {
			System.err.println("No se puede abrir el fichero de log");
			e1.printStackTrace();
		}

		log.setUseParentHandlers(false); // To avoid console logging
		log.addHandler(fd);
		SimpleFormatter formatter = new SimpleFormatter();
		fd.setFormatter(formatter);

		// Initialize cluster

		String nodeName = props.nodeName;

		DBType type;
		if (props.ThisNode.equals("Master") || props.ThisNode.equals("MASTER"))
			type = DBType.MASTER;
		else
			type = DBType.REPLICA;

		Cluster cluster = new Cluster(log, nodeName, props.nodes, type);

		if (!cluster.setMaster()) {
			System.err.println("Cannot set cluster Master. Exiting...");
			log.log(Level.SEVERE, "Cannot set cluster Master. Exiting.");
			return;
		}

		log.log(Level.INFO, nodeName + " : This node acts as " + type);

		if (type == DBType.MASTER) {
			String[] temp = cluster.setReplicas();
			if (temp[0].equals("FAIL")) {
				System.err.println("Failing setup cluster " + temp[1]);
				log.log(Level.SEVERE, "Fail to setup cluster " + temp[1]);
				return;
			}
			System.err.println(temp[1]);
			if (cluster.liveReplicas.size() > 0) {
				log.log(Level.INFO, "Replicas living at this time:");
				for (Node n : cluster.liveReplicas)
					log.log(Level.INFO, n.name);

			}
			// Start the cluster daemon
			log.log(Level.INFO, "Starting WatchDog daemon for cluster maintenance");
			cluster.clusterWatchDog(props.pingTime, props.maxTicksDead);
		}

		// If I'm a replica. Joins the cluster

		if (type == DBType.REPLICA) {
			String[] temp = cluster.joinMeToCluster();
			if (temp[0].equals("FAIL")) {
				System.err.println("This replica cannot join the cluster. Exiting " + temp[1]);
				log.log(Level.SEVERE, "Replica cannot join the cluster. Exiting " + temp[1]);
				return;
			}
			log.log(Level.INFO, "Joined to cluster");
		}

		// Initialize Syncer
		log.log(Level.INFO, "Starting disk logger and net syncer");
		MasterSyncer dsync = null;
		ClusterHTTPServer clusterHTTPserver = null;

		if (type == DBType.MASTER) {
			dsync = new MasterSyncer(log, cluster, 1000 * props.syncNetTime);
			log.log(Level.INFO, "Starting syncer. Nett syncing each " + props.syncNetTime + " seconds");
		} else
			log.log(Level.INFO, "This node is a replica. Not syncer needed");
		
		
		// Initialize databases
		log.log(Level.INFO, "Initializing databases");
		dbs = new HashMap<>();

		for (Map<String, String> m : props.databases) {
			log.log(Level.INFO, "Initializing database " + m.get("name"));
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

		// Initialize the cluster HTTP Server
		clusterHTTPserver = new ClusterHTTPServer(log, props.clusterPort, cluster, dbs);

		// Start Syncer thread

		if (type == DBType.MASTER && dsync != null) {
			System.err.println("Starting the Syncer thread");
			Thread dSync = new Thread(dsync);
			dSync.setName("Master Syncer");
			dSync.start();
		}

		// Start the Cluster HTTP server
		Thread clusterServer = new Thread(clusterHTTPserver);
		clusterServer.setName("Cluster HTTP Server");
		clusterServer.start();

		// Start the HTTP Data Server

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

		System.out.println("Arrancando el servidor en el puerto: " + props.dataPort);
		log.log(Level.INFO, "Server started");
		log.log(Level.INFO, "Listening at port: " + props.dataPort);

		Socket client = null;
		while (keepRunning.get()) {
			try {
				client = server.accept();
			} catch (IOException e) {
				log.log(Level.WARNING, "Cannot launch thread to accept client: " + client.toString());
				log.log(Level.WARNING, Arrays.toString(e.getStackTrace()));
			}
			final DataServerAPI request = new DataServerAPI(log, client, props.adminRootPath, dbs, dsync);
			Thread thread = new Thread(request);
			thread.setName("Data Request Dispatcher #" + thread.getId());
			thread.start();
		}
	}

	private class Properties {
		String nodeName;
		String ThisNode;
		int dataPort;
		int clusterPort;
		int syncNetTime;
		int pingTime;
		int maxTicksDead;

		String adminRootPath;

		List<Map<String, String>> nodes;
		List<Map<String, String>> databases;
	}
}
