package com.distdb.dbserver;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class DistServer {

	static Logger log = Logger.getLogger("DistServer");
	public enum Type {
		MASTER,
		REPLICA
	}
	
	public static void main(String[] args) {
		System.setProperty("java.util.logging.SimpleFormatter.format", 
				"%1$tF %1$tT %4$s %5$s%6$s%n");
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
			e.printStackTrace();
		}
		
		Type type;
		
		if (props.ThisNode.equals("Master") || props.ThisNode.equals("MASTER"))
			type = Type.MASTER;
		else
			type = Type.REPLICA;
		
		for (Map<String, String> m: props.databases) {
			System.err.println("Database: " + m.get("Name"));
			DBServer db =  new DBServer(log, props.databases, type);
		}

	}
	
	private class Properties {
		String ThisNode;
		int  dataPort;
		int clusterPort;
		List<Map<String, String>> nodes;
		List<Map<String, String>> databases;
	}
}
