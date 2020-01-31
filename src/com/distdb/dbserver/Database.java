package com.distdb.dbserver;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import com.distdb.dbserver.DistServer.DBType;
import com.distdb.dbsync.DiskSyncer;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public class Database {

	private Logger log;
	private String dbname;
	private String propsFile;
	private DBType type;
	private DiskSyncer dSyncer;

	private Map<String, DBObject> dbobjs;

	public Database(Logger log, String name, String config, String defPath, DiskSyncer dSyncer, DBType type) {
		System.err.println("Opening " + (type == DBType.MASTER ? "MASTER": "REPLICA") + " database " + name + " with file " + config + " at " + defPath);

		this.propsFile = "etc/config/" + config;
		this.log = log;
		this.type = type;
		this.dSyncer = dSyncer;
		dbname = name;
		dbobjs = new HashMap<>();

		// Read the properties
		Gson json = new Gson();
		Properties props = null;
		try {
			props = json.fromJson(new FileReader(propsFile), Properties.class);
		} catch (JsonSyntaxException | JsonIOException | FileNotFoundException e) {
			e.printStackTrace();
		}

		for (String s : props.objects) {
			System.err.println("Instanciando la colección de objetos: " + s);
			System.err.println("Datafile: " + props.dataPath + "/" + "_data_" + s);
			Class cl;
			try {
				cl = Class.forName(defPath + "." + s);
				DBObject dbo = new DBObject(props.dataPath + "/" + "_data_" + s, cl, type, log);
				dbobjs.put(s, dbo);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

	}

	public void open() {

	}

	public String[] close() {
		String ret[] = new String[2];
		if (type == DBType.MASTER) {
			String[] temp = new String[2];
			ret[0] = "OK";
			ret[1] = "";
			for (DBObject o : dbobjs.values()) {
				temp = new String[2];
				temp = o.flush();
				if (!temp[0].equals("OK"))
					ret[0] = temp[0];
			}
		} else {
			ret[0] = "FAIL";
			ret[1] = "Replicas cannot save database on close";
		}
		
		for (DBObject o : dbobjs.values())
			o.close();
		dbobjs = null;
		System.err.println("Cerrando la base de datos " + dbname);
		return ret;
	}

	public String[] sync() {
		String ret[] = new String[2];
		if (type == DBType.REPLICA) {
			
		} else {
			ret[0] = "FAIL";
			ret[1] = "Only replicas must sync";
		}
		return ret;
	}

	public String[] insert(String objectName, Object o) {
		String ret[] = new String[2];
		Class<?> cl = o.getClass();
		if (!objectName.equals(cl.getName())) {
			ret[0] = "FAIL";
			ret[1] = "Invalid Object";
		}
		Field f = null;
		String id = null;
		try {
			Class spcl = cl.getSuperclass();
			f = spcl.getDeclaredField("id");
			id = (String) f.get(o);
			f = spcl.getDeclaredField("onDisk");
			f.set(o, false);
			dSyncer.addObject(dbname, objectName, id, o);
		} catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
			System.err.println("Algo fue mal con el obeto a insertar");
			ret[0] = "FAIL";
			ret[1] = "Something went wrong with the object to insert in the Database";
			e.printStackTrace();
			return ret;
		}

		dbobjs.get(objectName).insert(id, o);
		ret[0] = "OK";
		ret[1] = "Inserted new " + objectName + " with id " + id;
		return ret;
	}

	public String[] remove(String objectName, String id) {
		String ret[] = new String[2];
		ret[0] = "FAIL";
		ret[1] = "Object does not exist";
		if (dbobjs.get(objectName).getById(id) != null) {
			dbobjs.get(objectName).remove(id);
			ret[0] = "OK";
			ret[1] = "Remove object with id " + id;
		}
		return ret;
	}

	public Object getById(String objectName, String id) {
		return dbobjs.get(objectName).getById(id);
	}

	public List<Object> searchByField(String objectName, String fieldName, String value) {
		List<Object> ret = dbobjs.get(objectName).searchByField(fieldName, value);
		return ret;
	}

	public Map<String, String> getInfo() {
		Map<String, String> ret = new HashMap<>();
		ret.put("Name", dbname);
		ret.put("Config File: ", propsFile);
		for (String s : dbobjs.keySet()) {
			ret.put(s, Integer.toString(dbobjs.get(s).size()));
		}
		return ret;
	}

	private class Properties {
		String dataPath;
		List<String> objects;
	}

}
