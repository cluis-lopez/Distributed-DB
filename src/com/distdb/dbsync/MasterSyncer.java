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

	public Map<String, List<LoggedOps>> dbQueue;
	public Map<String, String> dataPaths;
	public int waitTime;
	private Cluster cluster;
	private Logger log;
	private AtomicBoolean keepRunning;

	public MasterSyncer(Logger log, Cluster cluster, int syncNetTime) {
		this.log = log;
		this.cluster = cluster;
		dbQueue = new HashMap<>();
		dataPaths = new HashMap<>();
		this.waitTime = syncNetTime;

		this.keepRunning.set(true);;
	}

	public void addDatabase(String database, Map<String, DBObject> objetos, String dataPath) {
		List<LoggedOps> queue = new ArrayList<>();
		dbQueue.put(database, queue);
		dataPaths.put(database, dataPath);
	}
	
	public void removeDatabase(String database){
		dbQueue.remove(database);
		dataPaths.remove(database);
	}

	public synchronized void enQueue(String operation, String database, String objectName, String id, Object o) {
		// Log the operation in the queue list for net syncing
		dbQueue.get(database).add(new LoggedOps(operation, objectName, id, o));
		// Then add the new operation to the logging file immediately
		String loggingFile = dataPaths.get(database) + "/" + database + "_logging";
		LoggedOps op = new LoggedOps(operation, objectName, id, o);
		appendJson(loggingFile, op);

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

	public Map<String, String> getInfoFromLogFiles() {
		Map<String, String> ret = new HashMap<>();
		for (String s : dbQueue.keySet()) {
			String dataPath = dataPaths.get(s);
			java.lang.reflect.Type dataType = TypeToken.getParameterized(List.class, LoggedOps.class).getType();
			Gson json = new Gson();
			int nInserts = 0;
			int nRemoves = 0;
			try {
				FileReader fr = new FileReader(dataPath + "/" + s + "_logging");
				List<LoggedOps> logged = json.fromJson(fr, dataType);
				fr.close();
				for (LoggedOps l : logged) {
					if (l.op.equals("insert"))
						nInserts++;
					if (l.op.equals("remove"))
						nRemoves++;
				}
			} catch (JsonIOException | JsonSyntaxException | IOException e) {
				System.err.println("No puedo accer al fichero de log, quizá no existe");
				log.log(Level.INFO, "Cannot read the log file. Maybe it's not there");
				log.log(Level.SEVERE, e.getMessage());
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
			}
			ret.put("Pending inserts for " + s, Integer.toString(nInserts));
			ret.put("Pending removes for " + s, Integer.toString(nRemoves));
		}
		return ret;
	}

	public void netSync() {
		if (cluster.liveReplicas.isEmpty()) // Nothing to do if there're no replicas to sync
			return;
		long oldestUpdate = System.currentTimeMillis();
		for (Node n : cluster.liveReplicas) { // Let's update each replica
			// cluster.liveReplicas.subList(0, 5).clear(); how to remove several list
			// entries
			if (n.lastUpdated < oldestUpdate)
				oldestUpdate = n.lastUpdated;
			for (String s : dbQueue.keySet()) {
				List<LoggedOps> tempList = new ArrayList<>();
				if (dbQueue.get(s).isEmpty())
					continue;
				for (int i = dbQueue.get(s).size(); i>=0; i--)
					if ()
						tempList.add(o);
					 
				updateNode(s, n, tempList);
			}
		}
	}


	private void updateNode(String dbName, Node n, List<LoggedOps> ops) {
		Gson json = new Gson();
		java.lang.reflect.Type dataType = TypeToken.getParameterized(List.class, LoggedOps.class).getType();
		String ret = HTTPDataMovers.postData(log, n.url, dbName, "sendUpdate", json.toJson(ops, dataType));
	}

	private boolean isEmpty() {
		boolean ret = true;
		for (String s : dbQueue.keySet()) {
			ret = ret && dbQueue.get(s).isEmpty();
		}
		return ret;
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
				fr.seek(f.length() - 2); // Nos posicionamos antes del último corchete
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
				fr.seek(f.length() - 2); // Nos posicionamos antes del último corchete
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
		public String op;
		public String objectName;
		public String id;
		public Object o;

		public LoggedOps(String operation, String objectName, String id, Object o) {
			this.timeStamp = System.currentTimeMillis();
			this.op = operation;
			this.objectName = objectName;
			this.id = id;
			this.o = o;
		}
	}
}
