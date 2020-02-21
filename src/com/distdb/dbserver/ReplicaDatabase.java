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
		for (String s : props.objects) {
			Class<?> cl;
			try {
				cl = Class.forName(defPath + "." + s);
				dbo = new DBObject(cl, log);
				askForObject(myMaster, cl, dbo);
				dbobjs.put(s, dbo);
			} catch (ClassNotFoundException e) {
				log.log(Level.SEVERE, "Cannot instantiate object collection for objects : " + s);
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
				ret[0] = "FAIL";
				ret[1] = "Cannot instantiate objects;";
				return ret;
			}
		}

		// Now asks for the Logging file and apply ops

		boolean result = true;
		try {
			String fr = askForLogging();
			JsonArray array = new JsonParser().parse(fr).getAsJsonArray();
			for (JsonElement jsonElement : array) {
				JsonObject jobj = new JsonParser().parse(jsonElement.toString()).getAsJsonObject();
				if ((jobj.get("op").getAsString()).equals("insert")) {
					Class<?> cl = Class.forName(defPath + "." + jobj.get("objectName").getAsString());
					Object object = new Gson().fromJson(jobj.get("o"), cl);
					insert(jobj.get("objectName").getAsString(), object, false);
				}
				if ((jobj.get("op").getAsString()).equals("remove"))
					remove(jobj.get("objectName").getAsString(), jobj.get("id").getAsString(), false);
			}
		} catch (JsonIOException | JsonSyntaxException | ClassNotFoundException e) {
			System.err.println("No se ha podido aplicar el log a la base de datos " + dbname);
			log.log(Level.SEVERE, "Cannot apply log to database " + dbname);
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
			;
		}
		if (!result) { // WARNING: implementar la gestión de errores
			System.err.println("No se han podido aplicar correctamente todos los log a la base de datos " + dbname);
			log.log(Level.SEVERE, "Cannot properly apply any log to database " + dbname);
		}
		return ret;
	}

	@Override
	public String[] close() {
		String[] ret = new String[3];
		ret[0] = "OK";ret[1] = "Database closed";

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
		ret[0] = "FAIL"; ret[1] = "Replica Database";
		return ret;
	}

	private void askForObject(URL myMaster, Class<?> cl, DBObject dbo) {
		JsonObject jo = new JsonObject();
		jo.addProperty("user", "");
		jo.addProperty("token", "");
		jo.addProperty("objectName", cl.getSimpleName());
		String retCodes = HTTPDataMovers.postData(log, myMaster, dbname, "getObjectFile", jo.toString());
		String[] codes = HelperJson.decodeCodes(retCodes);
		if (codes[0].equals("OK")) {
			java.lang.reflect.Type dataType = TypeToken.getParameterized(HashMap.class, String.class, cl).getType();
			dbo.obj = new Gson().fromJson(codes[3], dataType);
		}
	}

	private String askForLogging() {
		JsonObject jo = new JsonObject();
		jo.addProperty("user", "");
		jo.addProperty("token", "");
		String retCodes = HTTPDataMovers.postData(log, myMaster, dbname, "getLoggingFile", jo.toString());
		String[] codes = HelperJson.decodeCodes(retCodes);
		if (codes[0].equals("OK")) {
			return codes[3];
		}
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