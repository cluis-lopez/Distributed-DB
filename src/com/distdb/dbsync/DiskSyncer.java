package com.distdb.dbsync;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbserver.DBObject;

public class DiskSyncer implements Runnable {

	Map<String, Map<String, List<LoggedObj>>> addQueue; // LIFO queue to write changes on disk
	Map<String, Map<String, List<String>>> removeQueue; //LIFO to store the id's of invalid (removed) objects
	Map<String, String> dataPaths;
	int waitTime;
	Logger log;

	public DiskSyncer(Logger log, int waitTime) {
		this.log = log;
		addQueue = new HashMap<>();
		removeQueue = new HashMap<>();
		dataPaths = new HashMap<>();
		this.waitTime = waitTime;
	}

	public void addDatabase(String database, Map<String, DBObject> objetos, String dataPath) {
		Map<String, List<LoggedObj>>addedObjs = new HashMap<>();
		Map<String, List<String>> removedObjs= new HashMap<>();		
		for (String objectName : objetos.keySet()) {
			List<LoggedObj> addObjectList = new ArrayList<>();
			addedObjs.put(objectName, addObjectList);
			List<String> removeObjectList = new ArrayList<>();
			removedObjs.put(objectName, removeObjectList);
			
		}
		addQueue.put(database, addedObjs);
		removeQueue.put(database, removedObjs);
		dataPaths.put(database, dataPath);
	}

	public void addObject (String database,String objectName, String id, Object o) {
		addQueue.get(database).get(objectName).add(new LoggedObj(id, o));
	}

	@Override
	public void run() {
		while (true) { // NOSONAR
			if (addQueue.isEmpty() && removeQueue.isEmpty())
				break;
			for (String s : addQueue.keySet()) {
				log.log(Level.INFO, "Logging insertions for Database " + s);

			}
		}

	}

	private void appendJson() {

	}

	private class LoggedObj {
		String id;
		Object o;

		public LoggedObj(String id, Object o) {
			this.id = id;
			this.o = o;
		}
	}
}
