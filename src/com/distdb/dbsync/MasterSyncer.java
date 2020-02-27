package com.distdb.dbsync;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.HttpHelpers.HTTPDataMovers;
import com.distdb.dbserver.Cluster;
import com.distdb.dbserver.DBObject;
import com.distdb.dbserver.Node;
import com.distdb.dbsync.MasterSyncer.LoggedOps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public class MasterSyncer implements Runnable {

	public List<LoggedOps> logOps;
	public Map<String, String> dataPaths;
	public int waitTime;
	private Cluster cluster;
	private Logger log;
	private AtomicBoolean keepRunning;

	public MasterSyncer(Logger log, Cluster cluster, int syncNetTime) {
		this.log = log;
		this.cluster = cluster;
		logOps = new ArrayList<>();
		dataPaths = new HashMap<>();
		this.waitTime = syncNetTime;

		this.keepRunning.set(true);;
	}

	public void addDatabase(String database,  String dataPath) {
		dataPaths.put(database, dataPath);
	}
	
	public void removeDatabase(String database){
		dataPaths.remove(database);
	}

	public synchronized void enQueue(String operation, String database, String objectName, String id, Object o) {
		// Inmediately record the log on disk
		String loggingFile = dataPaths.get(database) + "/" + database + "_logging";
		LoggedOps op = new LoggedOps(database, operation, objectName, id, o);
		appendJson(loggingFile, op);
		// Then ... include the log inot the mem structure for later net syncing
		logOps.add(op);

	}

	public void kill() {
		this.keepRunning.set(false);
	}

	@Override
	public void run() {
		log.log(Level.INFO, "Starting the disk syncer daemon");
		log.log(Level.INFO, "Syncing replicas every " + waitTime / 1000 + " seconds");
		log.log(Level.INFO, "Syncing to disk each log as it happens. No delays");

		while (keepRunning.get()) {
			netSync();
			try {
				Thread.sleep(waitTime);
			} catch (InterruptedException e) {
			}
		}
	}

	public void netSync() {
		if (cluster.liveReplicas.isEmpty()) // Nothing to do if there're no replicas to sync
			return;
		long oldestUpdate = System.currentTimeMillis();
		int smallerIndex = 0;
		for (Node n : cluster.liveReplicas) { // Let's update each replica
			// cluster.liveReplicas.subList(0, 5).clear(); how to remove several list
			// entries
			List<LoggedOps> tempList = new ArrayList<>();
			if (n.lastUpdated < oldestUpdate)
				oldestUpdate = n.lastUpdated;
			for (int i = 0; i<logOps.size(); i++) //Recorremos la lista de operaciones pendientes
					 if (logOps.get(i).timeStamp< n.lastUpdated)
						 smallerIndex = i;
					 else
						 tempList.add(logOps.get(i));
			
				updateNode(n, tempList);
			}
		cluster.liveReplicas.subList(0, smallerIndex).clear(); //Trim the delayed ops list
	}


	private void updateNode(Node n, List<LoggedOps> ops) {
		Gson json = new Gson();
		java.lang.reflect.Type dataType = TypeToken.getParameterized(List.class, LoggedOps.class).getType();
		String ret = HTTPDataMovers.postData(log, n.url, "sendUpdate", json.toJson(ops, dataType));
	}

	private boolean appendJson(String loggingFile, LoggedOps objectToAppend) {
		boolean ret = false;
		Gson jsonWrite = new GsonBuilder().setPrettyPrinting().create();
		java.lang.reflect.Type dataType = TypeToken.getParameterized(LoggedOps.class).getType();
		dataType = new TypeToken<List<LoggedOps>>() {
		}.getType();
		File f = new File(loggingFile);
		String temp = jsonWrite.toJson(objectToAppend, dataType);
		try {
			if (f.exists()) {
				RandomAccessFile fr = new RandomAccessFile(f, "rw"); // The log file already exists
				fr.seek(f.length() - 2); // Nos posicionamos antes del �ltimo corchete
				fr.write((",\n" + temp.substring(2, temp.length() - 1) + "\n]").getBytes());
				fr.close();
			} else {
				FileWriter fw = new FileWriter(f);
				fw.write(temp);
				fw.close();
			}
			ret = true;
		} catch (IOException e) {
			System.err.println("Problemas al escribir el log");
			log.log(Level.WARNING, "Problems when updating or creating database log at " + loggingFile);
		}
		return ret;
	}

	private boolean appendBulkJson(String loggingFile, List<LoggedOps> objectsToAppend) {
		boolean ret = false;
		Gson jsonWrite = new GsonBuilder().setPrettyPrinting().create();
		java.lang.reflect.Type dataType = TypeToken.getParameterized(List.class, LoggedOps.class).getType();
		dataType = new TypeToken<List<LoggedOps>>() {
		}.getType();
		File f = new File(loggingFile);
		String temp = jsonWrite.toJson(objectsToAppend, dataType);
		try {
			if (f.exists()) {
				RandomAccessFile fr = new RandomAccessFile(f, "rw"); // The log file already exists
				fr.seek(f.length() - 2); // Nos posicionamos antes del �ltimo corchete
				fr.write((",\n" + temp.substring(2, temp.length() - 1) + "\n]").getBytes());
				fr.close();
			} else {
				FileWriter fw = new FileWriter(f);
				fw.write(temp);
				fw.close();
			}
			ret = true;
		} catch (IOException e) {
			System.err.println("Problemas al escribir el log");
			log.log(Level.WARNING, "Problems when updating or creating database log at " + loggingFile);
		}
		return ret;
	}

	public class LoggedOps {
		public long timeStamp;
		public String database;
		public String op;
		public String objectName;
		public String id;
		public Object o;

		public LoggedOps(String database, String operation, String objectName, String id, Object o) {
			this.timeStamp = System.currentTimeMillis();
			this.database = database;
			this.op = operation;
			this.objectName = objectName;
			this.id = id;
			this.o = o;
		}
	}
}
