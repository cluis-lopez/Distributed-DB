package com.distdb.dbserver;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.HttpHelpers.HTTPDataMovers;
import com.distdb.HttpHelpers.HelperJson;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public class ReplicaDatabase extends Database {

	private URL myMaster;

	public ReplicaDatabase(Logger log, String dbname, String config, String defPath, URL myMaster) {
		super(log, dbname, config, defPath);
		this.myMaster = myMaster;
	}

	@Override
	public String[] open() {
		String[] ret = new String[3];
		ret[0] = "OK";
		ret[1] = "Database " + dbname + " opened";
		DBObject dbo = null;

		int numDatafiles = 0;

		for (String s : props.objects) {
			Class<?> cl;
			try {
				cl = Class.forName(defPath + "." + s);
				dbo = new DBObject(cl, log);
				if (askForObject(myMaster, cl, dbo)) {
					;
					dbobjs.put(s, dbo);
					numDatafiles++;
				}
			} catch (ClassNotFoundException e) {
				log.log(Level.SEVERE, "Cannot instantiate object collection for objects : " + s);
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
				ret[0] = "FAIL";
				ret[1] = "Cannot instantiate objects;";
				return ret;
			}
		}

		if (numDatafiles == 0)
			log.log(Level.WARNING, dbname + ": No datafiles loaded ... maybe the masdter database was never cleanly closed");
		else if (numDatafiles == props.objects.size())
			log.log(Level.INFO, "All Datafiles successfully loaded");
		else { // Error esta BBDD no se puede cargar
			ret[0] = "FAIL";
			ret[1] = "Cannot load " + dbname;
			log.log(Level.WARNING, dbname + ": Something is incorrect in the datafile structure for this database and cannot be loaded");
			return ret;
		}

		// Now asks for the Logging file and apply logged operations

		String logs = askForLogging();

		if (logs.length() > 0) {
			// Decode return logs
			try {
				JsonArray array = new JsonParser().parse(logs).getAsJsonArray();
				log.log(Level.INFO, dbname + ": Updating the replica database with " + array.size() + " logged operations");
				for (JsonElement jsonElement : array) {
					JsonObject jobj = new JsonParser().parse(jsonElement.toString()).getAsJsonObject();
					if ((jobj.get("op").getAsString()).equals("insert")) {
						Class<?> cl = Class.forName(defPath + "." + jobj.get("objectName").getAsString());
						Object object = new Gson().fromJson(jobj.get("o"), cl);
						replicaUpdateInsert(jobj.get("objectName").getAsString(), object);
					}
					if ((jobj.get("op").getAsString()).equals("remove"))
						replicaUpdateRemove(jobj.get("objectName").getAsString(), jobj.get("id").getAsString());
				}
			} catch (JsonIOException | JsonSyntaxException | ClassNotFoundException e) {
				log.log(Level.SEVERE, "Cannot apply log to database " + dbname);
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
				ret[0] = "FAIL";
				ret[1] = "Cannot load " + dbname;
			}
		} else {
			log.log(Level.INFO, dbname + ": No Logging file. The database was properly shutdown or never used");
			ret[1] = "Database " + dbname + " opened without Log";
		}
		log.log(Level.INFO, dbname + ": Replica database opened with " + dbobjs.size() + " objects");
		for (String s : dbobjs.keySet()) {
			log.log(Level.INFO, dbname + ": Object collection: " + s + " contains " + dbobjs.get(s).size() + " objects");
		}
		return ret;
	}

	@Override
	public String[] close() {
		String[] ret = new String[3];
		ret[0] = "OK";
		ret[1] = "Database closed";

		for (DBObject o : dbobjs.values())
			o.close();
		dbobjs = null;

		log.log(Level.INFO, "Closing replica database: " + dbname);

		return ret;
	}

	@Override
	public String[] insert(String objectName, Object object, boolean logging) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] remove(String objectName, String id, boolean logging) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getMasterInfo() {
		String[] ret = new String[2];
		ret[0] = "FAIL";
		ret[1] = "Replica Database";
		return ret;
	}
	
	@Override
	public String[] replicaUpdateInsert(String objectName, Object object) {
		String[] ret = new String[3];
		Class<?> cl = null;
		try {
			cl = Class.forName(defPath+"."+objectName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		if (!cl.getSimpleName().equals(objectName)) {
			ret[0] = "FAIL";
			ret[1] = "Invalid Object";
			return ret;
		}
		
		Field f = null;
		String id = null;
		Object o = null;
		
		try {
			o = object;
			Class<?> spcl = cl.getSuperclass();
			f = spcl.getDeclaredField("id");
			id = (String) f.get(o);
		} catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
			ret[0] = "FAIL";
			ret[1] = "Something went wrong with the object to insert in the Database";
			log.log(Level.WARNING, "Something went wrong trying to insert object of class "+cl.getSimpleName()+" into replica database "+ dbname);
			log.log(Level.WARNING, Arrays.toString(e.getStackTrace()));
			return ret;
		}

		dbobjs.get(objectName).insert(id, o);
		
		ret[0] = "OK";
		ret[1] = "Inserted new " + objectName + " with id " + id;
		return ret;
	}
	
	@Override
	public String[] replicaUpdateRemove(String objectName, String id) {
		String[] ret = new String[3];
		ret[0] = "FAIL";
		ret[1] = "Object does not exist";
		if (dbobjs.get(objectName).getById(id) != null) {
			dbobjs.get(objectName).remove(id);
			ret[0] = "OK";
			ret[1] = "Removed object with id " + id;
		}
		return ret;
	}

	private boolean askForObject(URL myMaster, Class<?> cl, DBObject dbo) {
		boolean result = false;
		JsonObject jo = new JsonObject();
		jo.addProperty("user", "");
		jo.addProperty("token", "");
		jo.addProperty("objectName", cl.getSimpleName());
		String retCodes = HTTPDataMovers.postData(log, myMaster, dbname, "getObjectFile", jo.toString());
		String[] codes = HelperJson.decodeCodes(retCodes);
		if (codes[0].equals("OK")) {
			java.lang.reflect.Type dataType = TypeToken.getParameterized(HashMap.class, String.class, cl).getType();
			dbo.obj = new Gson().fromJson(codes[2], dataType);
			result = true;
		} else {
			result = false;
		}
		return result;
	}

	private String askForLogging() {
		JsonObject jo = new JsonObject();
		jo.addProperty("user", "");
		jo.addProperty("token", "");
		String retCodes = HTTPDataMovers.postData(log, myMaster, dbname, "getLoggingFile", jo.toString());
		String[] codes = HelperJson.decodeCodes(retCodes);
		if (codes[0].equals("OK"))
			return codes[2];
		else {
			log.log(Level.INFO, dbname + ": Asking the Master for log files: " + codes[0] + " : " + codes[1]);
			return "";
		}
	}

	protected void updateProps() {
		try {
			FileWriter fw = new FileWriter(propsFile);
			Gson json = new GsonBuilder().setPrettyPrinting().create();
			fw.write(json.toJson(props, Properties.class));
			fw.close();
		} catch (IOException e) {
			log.log(Level.SEVERE, "Cannot properly update the Properties file for " + dbname);
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
		}
	}

}