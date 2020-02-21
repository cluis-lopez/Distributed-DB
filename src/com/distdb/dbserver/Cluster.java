package com.distdb.dbserver;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbserver.DistServer.DBType;

public class Cluster {
	public URL myURL;
	public EnumMap<DBType, List<Node>> declaredNodes;
	public List<Node> liveReplicas;
	public Node myMaster;
	public DBType myType;
	
	public Cluster(Logger log, List<Map<String, String>> props, DBType type) {
		declaredNodes = new EnumMap<>(DBType.class);
		declaredNodes.put(DBType.MASTER, new ArrayList<>());
		declaredNodes.put(DBType.REPLICA, new ArrayList<>());
		
		liveReplicas = new ArrayList<>();
		this.myType = type;
		
		for (Map<String, String> m:  props) {
			try {			
				URL url = new URL(m.get("url"));
				if (m.get("name").equals("ThisNode"))
					this.myURL = url;
								
				DBType temp;
				if (m.get("nodeType").equals("Master") || m.get("nodeType").equals("MASTER"))
						temp = DBType.MASTER;
				else
					temp = DBType.REPLICA;
				

				
				Node node = new Node (m.get("name"), url, temp);
				System.err.println("Adding node "+node.name+" as "+node.dbtype);
				declaredNodes.get(node.dbtype).add(node);
			} catch (MalformedURLException e) {
				System.err.println("Nodo mal especificado (URL??)");
				log.log(Level.WARNING, "Incorrect node specification. Check URL for node" + m.get("name"));
			}
		}			
	}

	public boolean setMaster() {
		if (declaredNodes.get(DBType.MASTER).size() != 1) {
			System.err.println("Only single Master is supported at this release");
			return false; // Single Master supported at first release
		}
		
		this.myMaster = declaredNodes.get(DBType.MASTER).get(0);
		
		if ( myType == DBType.MASTER) {
			return true; //Soy el Master, nada que hacer
		}
		else if (myMaster.fullCheck()[0].equals("OK")) { //Soy una réplica chequeo si el Master está operativo. Si no hay master operativo no puedo trabajar
				return true;
		}
			System.err.println("Cannot set master at "+this.myMaster.name);
			return false; //Soy una réplica y el Master no está operativo
	}
	
	public String[] setReplicas(){
		String[] ret = new String[2];
		ret[0] = "OK"; ret[1] = "";
		if (myType == DBType.REPLICA) {
			ret[0] = "FAIL"; ret[1] = "A Replica cannot serve other replicas";
			return ret;
		}
		// I'm a Master so I must create a collection of living replicas to serve
		String[] temp = new String[2];
		for (Node n: declaredNodes.get(DBType.REPLICA) ) {
			if (n.fullCheck()[0].equals("OK")) {
				liveReplicas.add(n);
			}
		}
		if (liveReplicas.size() == 0)
			ret[1] ="No replicas to serve at this time. I'm a standalone Master";
		return ret;
	}
	
	public String[] replicaJoinsCluster(String replicaname) {
		String[] ret = new String[2];
		ret[0] = "FAIL"; ret[1] = "";
		if (myType == DBType.REPLICA) {
			ret[1] = "Invalid opertion. A Replica wants to join me while I'm a replica";
			return ret;
		}
		
			
	}
}
