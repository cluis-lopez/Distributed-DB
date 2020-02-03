package com.distdb.dbsync;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbserver.DBObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public class DiskSyncer implements Runnable {

	Map<String, List<LoggedOps>> dbQueue;
	Map<String, String> dataPaths;
	int waitTime;
	Logger log;
	int sequence;

	public DiskSyncer(Logger log, int waitTime) {
		this.log = log;
		dbQueue = new HashMap<>();
		dataPaths = new HashMap<>();
		this.waitTime = waitTime;
		sequence = 0;
	}

	public void addDatabase(String database, Map<String, DBObject> objetos, String dataPath) {
		List<LoggedOps> queue = new ArrayList<>();
		dbQueue.put(database, queue);
		dataPaths.put(database, dataPath);
	}

	public void enQueue(String operation, String database, String objectName, String id, Object o) {
		dbQueue.get(database).add(new LoggedOps(operation, objectName, id, o));
	}

	@Override
	public void run() {
		Gson json = new GsonBuilder().setPrettyPrinting().create();
		java.lang.reflect.Type dataType = TypeToken.getParameterized(HashMap.class, List.class, LoggedOps.class)
				.getType();
		while (true) { // NOSONAR
			if (dbQueue.isEmpty())
				break;
			for (String s : dbQueue.keySet()) {
				log.log(Level.INFO, "Logging delayed operations for Database " + s);
				String dataFile = dataPaths.get(s) + "_logging_" + sequence + "_";
				FileWriter fw;
				try {
					fw = new FileWriter(dataFile, false);
					fw.write(json.toJson(dbQueue, dataType));
					fw.close();
				} catch (IOException e) {
					log.log(Level.SEVERE, "Cannot update logging operations on file: " + dataPaths.get(s) + "_logging_"
							+ sequence + "_");
					log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
				}
			}

			try {
				Thread.sleep(waitTime);
			} catch (InterruptedException e) {
				log.log(Level.SEVERE, "Interrupted Syncer");
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
			}
		}
	}

	private void rebuildDatabase(String dbname) {

	}

	private class LoggedOps {
		String op;
		String objectName;
		String id;
		Object o;

		public LoggedOps(String operation, String objectName, String id, Object o) {
			this.op = operation;
			this.objectName = objectName;
			this.id = id;
			this.o = o;
		}
	}
}
