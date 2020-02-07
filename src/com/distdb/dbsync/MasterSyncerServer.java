package com.distdb.dbsync;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbserver.Node;
import com.distdb.dbserver.DistServer.DBType;

public class MasterSyncerServer implements Runnable {

	private int clusterPort;
	private Logger log;
	private boolean keepRunning;
	private Map<DBType, List<Node>> nodes;

	public MasterSyncerServer(int port, Map<DBType,List<Node>> nodes) {
		this.clusterPort = port;
		this.nodes = nodes;
	}

	@Override
	public void run() {
		ServerSocket server = null;
		try {
			server = new ServerSocket(clusterPort);
		} catch (IOException e) {
			log.log(Level.SEVERE, "Cannot start Cluster Server Socket at port: " + clusterPort);
			log.log(Level.SEVERE, e.getMessage());
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
			System.err.println("No se puede arrancar el cluster en el puerto " + clusterPort);
			e.printStackTrace();
		}

		System.out.println("Arrancando el cluster");
		log.log(Level.INFO, "Cluster started");
		log.log(Level.INFO, "Listening at port: " + clusterPort);

		Socket client = null;

		while (keepRunning) {
			try {
				client = server.accept();
			} catch (IOException e) {
				log.log(Level.WARNING, "Cannot launch thread to accept client: " + client.toString());
				log.log(Level.WARNING, Arrays.toString(e.getStackTrace()));
			}
			final ClusterHTTPRequest request = new ClusterHTTPRequest(log, client);
			Thread thread = new Thread(request);
			thread.start();
		}
	}

}
