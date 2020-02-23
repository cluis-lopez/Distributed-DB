package com.distdb.dbserver;

import java.io.FileWriter;
import java.io.IOException;
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
			log.log(Level.WARNING, "No datafiles loaded ... maybe the masdter database was never cleanly closed");
		else if (numDatafiles == props.objects.size())
			log.log(Level.INFO, "All Datafiles successfully loaded");
		else { // Error esdta BBDD no se puede cargar
			ret[0] = "FAIL";
			ret[1] = "Cannot load " + dbname;
			return ret;
		}

		// Now asks for the Logging file and apply logged operations

		String logs = askForLogging();

		if (logs.length() > 0) {
			// Decode return logs
			try {
				JsonArray array = new JsonParser().parse(logs).getAsJsonArray();
				if (array.get(0).getAsString().equals("OK")) {
					for (JsonElement jsonElement : array.get(2).getAsJsonArray()) {
						JsonObject jobj = new JsonParser().parse(jsonElement.toString()).getAsJsonObject();
						if ((jobj.get("op").getAsString()).equals("insert")) {
							Class<?> cl = Class.forName(defPath + "." + jobj.get("objectName").getAsString());
							Object object = new Gson().fromJson(jobj.get("o"), cl);
							insert(jobj.get("objectName").getAsString(), object, false);
						}
						if ((jobj.get("op").getAsString()).equals("remove"))
							remove(jobj.get("objectName").getAsString(), jobj.get("id").getAsString(), false);
					}
				} else if (array.get(1).getAsString().equals("No File")) {

				}
			} catch (JsonIOException | JsonSyntaxException | ClassNotFoundException e) {
				System.err.println("No se ha podido aplicar el log a la base de datos " + dbname);
				log.log(Level.SEVERE, "Cannot apply log to database " + dbname);
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
				ret[0] = "FAIL";
				ret[1] = "Cannot load " + dbname;
			}
		} else {
			System.err.println("No Logging file. The database was properly shutdown or never used");
			log.log(Level.INFO, "No Logging file. The database was properly shutdown or never used");
			ret[1] = "Database " + dbname + " opened without Log";
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

		System.err.println("Cerrando la base de datos replica " + dbname);
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
			return codes[3];
		else
			return "";
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

}