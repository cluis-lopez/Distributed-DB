package com.distdb.dbserver;

import java.io.File;
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
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.distdb.dbsync.DiskSyncer.LoggedOps;

public class Database {

	private Logger log;
	public String dbname;
	public String dataPath;
	private String propsFile;
	private DBType type;
	private DiskSyncer dSyncer;
	private Properties props;

	public Map<String, DBObject> dbobjs;

	public Database(Logger log, String name, String config, String defPath, DiskSyncer dSyncer, DBType type) {
		System.err.println("Opening " + (type == DBType.MASTER ? "MASTER" : "REPLICA") + " database " + name
				+ " with file " + config + " at " + defPath);

		this.propsFile = "etc/config/" + config;
		this.log = log;
		this.type = type;
		this.dSyncer = dSyncer;
		dbname = name;
		dbobjs = new HashMap<>();

		// Read the properties
		Gson json = new Gson();
		this.props = null;
		try {
			props = json.fromJson(new FileReader(propsFile), Properties.class);
		} catch (JsonSyntaxException | JsonIOException | FileNotFoundException e) {
			log.log(Level.SEVERE, "Cannot read definition file for database : " + dbname);
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
		}

		this.dataPath = props.dataPath;
		DBObject dbo = null;
		for (String s : props.objects) {
			Class cl;
			try {
				cl = Class.forName(defPath + "." + s);
				dbo = new DBObject(dataPath + "/" + "_data_" + s, cl, type, log);
				dbobjs.put(s, dbo);
			} catch (ClassNotFoundException e) {
				log.log(Level.SEVERE, "Cannot instantiate object collection for objects : " + s);
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
			}
		}

		if (!props.isProperlyShutdown) { // La BBDD no se ha apagado correctamente asi que debemos aplicar los logs
			boolean result = true;
			json = new Gson();
			java.lang.reflect.Type dataType = TypeToken.getParameterized(List.class, LoggedOps.class).getType();
			try {
				String [] ret = new String[2];;
				List<LoggedOps> logs = json.fromJson(new FileReader(dataPath + "/" + dbname +"_logging"), dataType);
				for(LoggedOps dblog: logs) {
					if (dblog.op.equals("insert"))
						ret = insert(dblog.objectName, dblog.o, false);
					if (dblog.op.equals("remove"))
						ret = remove(dblog.objectName, dblog.id, false);
					result = result && (ret[0].equals("OK"));
				}
			} catch (JsonIOException | JsonSyntaxException | FileNotFoundException e) {
				System.err.println("No se ha podio aplicar el log a la base de datos "+ dbname);
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
		String ret[] = new String[2];
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

	public String[] insert(String objectName, Object o, boolean logging) {
		String ret[] = new String[2];
		Class<?> cl = o.getClass();
		if (!objectName.equals(cl.getSimpleName())) {
			ret[0] = "FAIL";
			ret[1] = "Invalid Object";
			return ret;
		}
		Field f = null;
		String id = null;
		try {
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
		ret[0] = "OK";
		ret[1] = "Inserted new " + objectName + " with id " + id;
		return ret;
	}

	public String[] remove(String objectName, String id) {
		return remove(objectName, id, true);
	}

	public String[] remove(String objectName, String id, boolean logging) {
		String ret[] = new String[2];
		ret[0] = "FAIL";
		ret[1] = "Object does not exist";
		if (dbobjs.get(objectName).getById(id) != null) {
			dbobjs.get(objectName).remove(id);
			if (logging && type == DBType.MASTER && dSyncer != null)
				dSyncer.enQueue("remove", dbname, objectName, id, null);
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
