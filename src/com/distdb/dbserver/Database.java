package com.distdb.dbserver;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbsync.DiskSyncer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

public abstract class Database {

	public String dbname;
	public String defPath;
	protected Logger log;
	protected String propsFile;
	protected Properties props;

	public Map<String, DBObject> dbobjs;
	
	public Database(Logger log, String dbname, String config, String defPath) {
		System.err.println("Opening  database " + dbname
				+ " using config file " + config + " and objects defined in " + defPath);

		this.propsFile = "etc/config/" + config;
		this.log = log;
		this.defPath = defPath;
		this.dbname = dbname;
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
	}
	
	public abstract String[] open();
	public abstract String[] close();
	public abstract String[] sync();
	public abstract String[] insert(String objectName, Object object, boolean logging);
	public abstract String[] remove(String objectName, String id, boolean logging);
	
	public String[] insert(String objectName, Object o) {
		return insert(objectName, o, true);
	}

	public String[] remove(String objectName, String id) {
		return remove(objectName, id, true);
	}
	
	public String[] getById(String objectName, String id) {
		String[] ret = new String[3];
		ret[0] = "FAIL";
		ret[1] = "No object " + objectName + " with id " + id;
		if (id == null || objectName == null || id.equals("") || objectName.equals("")
				|| dbobjs.get(objectName) == null)
			return ret;

		try {
			Class<?> cl = Class.forName(defPath + "." + objectName);
			Object object = cl.cast(dbobjs.get(objectName).getById(id));
			ret[2] = new Gson().toJson(object, cl);
		} catch (ClassNotFoundException e) {
			ret[0] = "FAIL";
			ret[1] = "Cannot find class descriptor for " + objectName;
			return ret;
		}
		ret[0] = "OK";
		ret[1] = "Returned object with id " + id;
		return ret;
	}
	
	public String[] searchByField(String objectName, String fieldName, String value) {
		String[] ret = new String[3];
		ret[0] = "FAIL";
		ret[1] = "No object " + objectName + " or fieldname " + fieldName;
		if (fieldName == null || objectName == null || fieldName.equals("") || objectName.equals("")
				|| dbobjs.get(objectName) == null)
			return ret;

		List<Object> temp = dbobjs.get(objectName).searchByField(fieldName, value);

		if (temp.isEmpty()) {
			ret[1] = "Cannot find any object " + objectName + " with the pattern " + value + " on field " + fieldName;
		} else {
			ret[0] = "OK";
			ret[1] = "Find " + temp.size() + " objects matching";
		}

		ret[2] = new Gson().toJson(temp, List.class);
		return ret;
	}
	
	public String getInfo() {
		JsonObject jo = new JsonObject();
		jo.addProperty("Name", dbname);
		//jo.addProperty("Config File", dataPath);
		jo.addProperty("DB Last properly shutdown",  new GsonBuilder().setDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz").create().toJson(props.lastProperlyShtdown));
		for (String s : dbobjs.keySet()) {
			jo.addProperty(s, dbobjs.get(s).size());
		}
		return jo.toString();
	}

	protected void updateProps() {
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

	protected class Properties {
		String dataPath;
		boolean isProperlyShutdown;
		Date lastProperlyShtdown;
		boolean isAvailable;
		boolean isReadOnly;

		List<String> objects;
	}
}
