package com.distdb.dbsync;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
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
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public class DiskSyncer implements Runnable {

	public Map<String, List<LoggedOps>> dbQueue;
	public Map<String, String> dataPaths;
	public int waitTime;
	private Logger log;
	private boolean keepRunning;

	public DiskSyncer(Logger log, int waitTime) {
		this.log = log;
		dbQueue = new HashMap<>();
		dataPaths = new HashMap<>();
		this.waitTime = waitTime;
		this.keepRunning = true;
	}

	public void addDatabase(String database, Map<String, DBObject> objetos, String dataPath) {
		List<LoggedOps> queue = new ArrayList<>();
		dbQueue.put(database, queue);
		dataPaths.put(database, dataPath);
	}

	public void enQueue(String operation, String database, String objectName, String id, Object o) {
		dbQueue.get(database).add(new LoggedOps(operation, objectName, id, o));
	}

	public void kill() {
		this.keepRunning = false;
	}

	@Override
	public void run() {
		while (keepRunning) {
			forceLog();
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
			} catch (JsonIOException | JsonSyntaxException |IOException e) {
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

	private void rebuildDatabaseOnDisk(String dbname) {

	}

	public void forceLog() {
		for (String s : dbQueue.keySet()) {
			if (dbQueue.get(s).isEmpty())
				continue;
			log.log(Level.INFO, "Logging " + dbQueue.get(s).size() + "  delayed operations for Database " + s);
			String dataFile = dataPaths.get(s) + "/" + s + "_logging";
			if (!appendJson(dataFile, dbQueue.get(s))) {
				log.log(Level.WARNING, "Cannot update log file");
				System.err.println("No se puede actualizar el fichero de log para la base de datos " + s);
			} else {
				dbQueue.get(s).clear(); // Se vacia la pila de log correspondiente a la BBDD
			}
			log.log(Level.INFO, "Logged  operations for Database " + s);
		}
	}

	private boolean isEmpty() {
		boolean ret = true;
		for (String s : dbQueue.keySet()) {
			ret = ret && dbQueue.get(s).isEmpty();
		}
		return ret;
	}

	private boolean appendJson(String dataFile, List<LoggedOps> objectToAppend) {
		boolean ret = false;
		Gson jsonWrite = new GsonBuilder().setPrettyPrinting().create();
		java.lang.reflect.Type dataType = TypeToken.getParameterized(List.class, LoggedOps.class).getType();
		File f = new File(dataFile);
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
			log.log(Level.WARNING, "Problems when updating or creating database log at " + dataFile);
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
