package com.distdb.dbserver;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbserver.DistServer.DBType;
import com.distdb.dbsync.DiskSyncer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public class Database {

	public String dbname;
	public String dataPath;
	public String defPath;
	private Logger log;
	private String propsFile;
	private DBType type;
	private DiskSyncer dSyncer;
	private Properties props;

	public Map<String, DBObject> dbobjs;

	/**
	 * @param log
	 * @param name
	 * @param config
	 * @param defPath
	 * @param dSyncer
	 * @param type
	 */
	public Database(Logger log, String name, String config, String defPath, DiskSyncer dSyncer, DBType type) {
		System.err.println("Opening " + (type == DBType.MASTER ? "MASTER" : "REPLICA") + " database " + name
				+ " with file " + config + " at " + defPath);

		this.propsFile = "etc/config/" + config;
		this.log = log;
		this.type = type;
		this.dSyncer = dSyncer;
		this.defPath = defPath;
		dbname = name;
		dbobjs = new HashMap<>();

		// Read the properties
		Gson json = new Gson();
		this.props = null;
		try {
			props = json.fromJson(new FileReader(propsFile), Properties.class);
		} catch (JsonSyntaxException | JsonIOException | FileNotFoundException e) {
			log.log(Level.SEVERE, "Cannot read definition file for database : " + dbname);
			log.log(Level.SEVERE, e.getMessage());
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
		}

		this.dataPath = props.dataPath;
		DBObject dbo = null;
		for (String s : props.objects) {
			Class<?> cl;
			try {
				cl = Class.forName(defPath + "." + s);
				dbo = new DBObject(dataPath + "/" + "_data_" + s, cl, type, log);
				dbobjs.put(s, dbo);
			} catch (ClassNotFoundException e) {
				log.log(Level.SEVERE, "Cannot instantiate object collection for objects : " + s);
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
			}
		}

		if (!props.isProperlyShutdown && type == DBType.MASTER) { // La BBDD no se ha apagado correctamente asi que debemos aplicar los logs
			log.log(Level.INFO, "Database was not properly shutdown. Recovering from logs if any");
			boolean result = true;
			try {
				FileReader fr = new FileReader(dataPath + "/" + dbname +"_logging");
				JsonArray array = new JsonParser().parse(fr).getAsJsonArray();
				for (JsonElement jsonElement : array) {
		            JsonObject jobj = new JsonParser().parse(jsonElement.toString()).getAsJsonObject();
		            if((jobj.get("op").getAsString()).equals("insert")) {
		            	Class<?> cl = Class.forName(defPath + "." + jobj.get("objectName").getAsString());
		            	Object object = new Gson().fromJson(jobj.get("o"), cl);
		            	insert(jobj.get("objectName").getAsString(), object, false);
		            }
		            if((jobj.get("op").getAsString()).equals("remove"))
		            	remove(jobj.get("objectName").getAsString(), jobj.get("id").getAsString(), false);
				}
				fr.close();
			} catch (JsonIOException | JsonSyntaxException | ClassNotFoundException | IOException e) {
				System.err.println("No se ha podido aplicar el log a la base de datos "+ dbname);
				log.log(Level.SEVERE, "Cannot apply log to database "+dbname);
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));;
			}
			if (!result) {
				System.err.println("No se han podido aplicar correctamente todos los log a la base de datos "+ dbname);
				log.log(Level.SEVERE, "Cannot properly apply any log to database "+dbname);
			}
		}

		dSyncer.addDatabase(dbname, dbobjs, dataPath + "/");
		props.isProperlyShutdown = false;
		updateProps();
	}

	public void open() {

	}

	public String[] close() {
		String[] ret = new String[3];
		ret[0] = "OK";
		String[] temp;
		if (type == DBType.MASTER) {
			for (DBObject o : dbobjs.values()) {
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
		log.log(Level.INFO, "Closing database: " + dbname);

		if (ret[0].equals("OK")) {
			dSyncer.forceLog();
			props.isProperlyShutdown = true;
			updateProps();
			Path path = Paths.get(dataPath +"/"+ dbname +"_logging");
			if (Files.isRegularFile(path)) {
				try {
					Files.delete(path);
				} catch (IOException e) {
					log.log(Level.SEVERE, "Cannot delete logging for database "+ dbname);
					log.log(Level.SEVERE, e.getMessage());
					log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
				}
				log.log(Level.INFO, "Properly removed logging file for database "+ dbname);;
			}
		}
		return ret;
	}

	public String[] sync() {
		String[] ret = new String[2];
		ret[0] ="OK"; ret[1] ="Syncing Replica";
		if (type == DBType.REPLICA) {

		} else {
			ret[0] = "FAIL";
			ret[1] = "Only replicas must sync";
		}
		return ret;
	}

	public String[] insert(String objectName, Object o) {
		return insert(objectName, o, true);
	}

	public String[] insert(String objectName, Object object, boolean logging) {
		String[] ret = new String[3];
		Class<?> cl = object.getClass();
		Field f = null;
		String id = null;
		Object o = null;
		
		if (!cl.getSimpleName().equals(objectName)) {
			ret[0] = "FAIL";
			ret[1] = "Invalid Object";
			return ret;
		}
		
		try {
			o = object;
			System.out.println(objectName + " : "+ cl.getSimpleName());
			Class spcl = cl.getSuperclass();
			f = spcl.getDeclaredField("id");
			id = (String) f.get(o);
			f = spcl.getDeclaredField("onDisk");
			f.set(o, !logging);
		} catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
			System.err.println("Algo fue mal con el objeto a insertar");
			ret[0] = "FAIL";
			ret[1] = "Something went wrong with the object to insert in the Database";
			e.printStackTrace();
			return ret;
		}

		dbobjs.get(objectName).insert(id, o);

		if (logging && type == DBType.MASTER && dSyncer != null)
			dSyncer.enQueue("insert", dbname, objectName, id, o);
		ret[0] ="OK";
		ret[1] = "Inserted new " + objectName + " with id " + id;
		return ret;
	}

	public String[] remove(String objectName, String id) {
		return remove(objectName, id, true);
	}

	public String[] remove(String objectName, String id, boolean logging) {
		String[] ret = new String[3];
		ret[0] = "FAIL"; ret[1] = "Object does not exist";
		if (dbobjs.get(objectName).getById(id) != null) {
			dbobjs.get(objectName).remove(id);
			if (logging && type == DBType.MASTER && dSyncer != null)
				dSyncer.enQueue("remove", dbname, objectName, id, null);
			ret[0] ="OK";
			ret[1] = "Remove object with id " + id;
		}
		return ret;
	}

	public String[] getById(String objectName, String id) {
		String[] ret = new String[3];
		ret[0] = "FAIL"; ret[1] =  "No object "+objectName+" with id "+id;
		if (id == null || objectName == null || id.equals("") || objectName.equals("") || dbobjs.get(objectName) == null)
			return ret;

		try {
			Class<?> cl = Class.forName(defPath + "." +objectName);
			Object object = cl.cast(dbobjs.get(objectName).getById(id));
			ret[2] = new Gson().toJson(object, cl);
		} catch (ClassNotFoundException e) {
			ret[0] = "FAIL";
			ret[1] = "Cannot find class descriptor for " + objectName;
			return ret;
		}
		ret[0] = "OK";
		ret[1] = "Returned object with id "+id;
		return ret;
	}

	public String[] searchByField(String objectName, String fieldName, String value) {
		String[] ret = new String[3];
		ret[0] = "FAIL"; ret[1] = "No object "+objectName+" or fieldname "+fieldName;
		if (fieldName == null || objectName == null || fieldName.equals("") || objectName.equals("") || dbobjs.get(objectName) == null)
			return ret;
		
		List<Object> temp = dbobjs.get(objectName).searchByField(fieldName, value);
		
		if (temp.isEmpty()) {
			ret[1] = "Cannot find any object "+objectName+" with the pattern "+value+" on field "+fieldName;
		} else {
			ret[0] = "OK";
			ret[1] = "Find "+temp.size()+" objects matching";
		}
		
		ret[2] = new Gson().toJson(temp, List.class);
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

	private void updateProps() {
		try {
			FileWriter fw = new FileWriter(propsFile);
			Gson json = new GsonBuilder().setPrettyPrinting().create();
			fw.write(json.toJson(props, Properties.class));
			fw.close();
		} catch (IOException e) {
			log.log(Level.SEVERE, "Cannot prtoperly update the Properties file for " + dbname);
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
		}
	}

	private class Properties {
		String dataPath;
		boolean isProperlyShutdown;
		boolean isAvailable;
		boolean isReadOnly;

		List<String> objects;
	}

}
