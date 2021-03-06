package com.distdb.dbserver;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.HttpHelpers.HTTPDataMovers;
import com.distdb.dbserver.DistServer.DBType;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Cluster {
	private Logger log;
	public URL myURL;
	public EnumMap<DBType, List<Node>> declaredNodes;
	public List<Node> liveReplicas;
	public Node myMaster;
	public DBType myType;
	public String myName;

	public Cluster(Logger log, String nodeName, List<Map<String, String>> nodes, DBType type) {
		declaredNodes = new EnumMap<>(DBType.class);
		declaredNodes.put(DBType.MASTER, new ArrayList<>());
		declaredNodes.put(DBType.REPLICA, new ArrayList<>());

		liveReplicas = new ArrayList<>();
		this.myType = type;
		this.myName = nodeName;
		this.log = log;

		for (Map<String, String> n : nodes) {
			try {
				URL url = new URL(n.get("url"));
				if (n.get("name").equals(myName)) {
					this.myURL = url;
					continue; // Do not include myself in the nodes structure
				}

				DBType temp;
				if (n.get("nodeType").equals("Master") || n.get("nodeType").equals("MASTER"))
					temp = DBType.MASTER;
				else
					temp = DBType.REPLICA;

				Node node = new Node(log, n.get("name"), url, temp);
				log.log(Level.INFO, "Adding node " + node.name + " as " + node.dbtype);
				declaredNodes.get(node.dbtype).add(node);

			} catch (MalformedURLException e) {
				System.err.println("Nodo " + n.get("name") + " mal especificado (URL??)");
				log.log(Level.WARNING, "Incorrect node specification. Check URL for node" + n.get("name"));
			}
		}
	}

	public void clusterWatchDog(int pingTime, int maxTicksDead) {
		if (myType == DBType.REPLICA) //WatchDog only implemented in Masters
			return;
		WatchDog wd = new WatchDog(log, declaredNodes, liveReplicas, pingTime*1000, maxTicksDead);
		Thread wdt = new Thread(wd);
		wdt.setName("Cluster WatchDog");
		wdt.start();
	}

	public boolean setMaster() {
		if (myType == DBType.MASTER)
			return true; // Soy el Master, nada que hacer

		if (declaredNodes.get(DBType.MASTER).size() != 1) {
			System.err.println("No Master declared or more than one. Single Master is supported only at this release");
			log.log(Level.SEVERE, "No Master declared or more than one. Single Master is supported only at this release");
			log.log(Level.SEVERE, "Exiting");
			return false; // Single Master supported at first release
		}

		myMaster = declaredNodes.get(DBType.MASTER).get(0);

		if (myMaster.fullCheck()[0].equals("OK")) { // Soy una replica chequeo si el Master est� operativo. Si no
													// hay master operativo no puedo trabajar
			return true;
		}
		System.err.println("Cannot set master at " + this.myMaster.name);
		log.log(Level.SEVERE, "Cannot reach my master. A replica cannot operate with a living master");
		log.log(Level.SEVERE, "Exiting");
		return false; // Soy una r�plica y el Master no est� operativo
	}

	public String[] setReplicas() {
		String[] ret = new String[2];
		ret[0] = "OK";
		ret[1] = "";
		if (myType == DBType.REPLICA) {
			ret[0] = "FAIL";
			ret[1] = "A Replica cannot serve other replicas";
			return ret;
		}
		// I'm a Master so I must create a collection of living replicas to serve
		for (Node n : declaredNodes.get(DBType.REPLICA)) {
			if (n.fullCheck()[0].equals("OK")) {
				liveReplicas.add(n);
			}
		}
		if (liveReplicas.size() == 0)
			ret[1] = "No replicas to serve at this time. I'm a standalone Master";
		return ret;
	}

	public String[] joinMeToCluster() { // Only invoked by replicas
		String[] ret = new String[2];
		ret[0] = "FAIL";
		ret[1] = "";
		if (myType == DBType.MASTER) {
			ret[1] = "Invalid operation. A Master cannot join a cluster as replica";
			return ret;
		}
		// Check first if replica is not already in the alive structure
		for (Node n : liveReplicas) {
			if (n.name.equals(myName)) {
				ret[1] = "Something went wrong ... but I'm already in the cluster alive nodes";
				return ret;
			}
		}

		JsonObject jo = new JsonObject();
		jo.addProperty("user", "");
		jo.addProperty("token", "");
		jo.addProperty("replicaName", myName);
		String s = HTTPDataMovers.postData(log, myMaster.url, "", "joinCluster", jo.toString());
		// Modify for retrying ...
		JsonElement je = new JsonParser().parse(s);
		ret[0] = je.getAsJsonArray().get(0).getAsString();
		ret[1] = je.getAsJsonArray().get(1).getAsString();
		return ret;
	}

	public String[] replicaWantsToJoinCluster(String replicaName) { // Only used by masters
		String[] ret = new String[2];
		ret[0] = "FAIL";
		ret[1] = "";
		if (myType == DBType.REPLICA) {
			ret[1] = "Invalid opertion. A Replica wants to join me, while I'm a replica";
			return ret;
		}

		// Check first if replica is not already in the alive structure
		for (Node n : liveReplicas) {
			if (n.name.equals(replicaName)) {
				ret[1] = "This replica is already in the living replicas structure";
				return ret;
			}
		}
		Node newReplica = null;
		for (Node n : declaredNodes.get(DBType.REPLICA)) {
			if (n.name.equals(replicaName))
				newReplica = n;
		}

		if (newReplica == null) {
			ret[1] = "The node " + replicaName + " does not belong to this cluster";
			return ret;
		}

		newReplica.isLive = true;
		newReplica.lastReached = System.currentTimeMillis();
		newReplica.lastUpdated = System.currentTimeMillis();
		liveReplicas.add(newReplica);
		log.log(Level.INFO, "The node " + replicaName + " has joined the cluster");
		ret[0] = "OK";
		return ret;
	}

	public String[] replicaWantsToLeaveCluster(String replicaName) { // Only used by masters
		String[] ret = new String[2];
		ret[0] = "FAIL";
		ret[1] = "";
		if (myType == DBType.REPLICA) {
			ret[1] = "Invalid opertion. A Replica wants to join me, while I'm a replica";
			return ret;
		}

		// Check if replica is in the alive structure
		Node replicaToRetire = null;
		for (Node n : liveReplicas) {
			if (n.name.equals(replicaName)) {
				replicaToRetire = n;
				break;
			}
		}
		if (replicaToRetire == null) {
			ret[1] = "The replica is not in the cluster";
			return ret;
		}

		replicaToRetire.isLive = false;
		liveReplicas.remove(replicaToRetire);
		log.log(Level.INFO, "The node " + replicaName + " has left the cluster");
		ret[0] = "OK";
		return ret;
	}

}