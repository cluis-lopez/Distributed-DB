package com.distdb.dbserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbsync.MasterSyncer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public class MasterDatabase extends Database {

	public String dataPath;
	public MasterSyncer dSyncer;

	/**
	 * @param log
	 * @param name
	 * @param config
	 * @param defPath
	 * @param dSyncer
	 * @param type
	 */
	public MasterDatabase(Logger log, String dbname, String config, String defPath, MasterSyncer dSyncer) {
		super(log, dbname, config, defPath);
		this.dSyncer = dSyncer;
		this.dataPath = props.dataPath;
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
				loadObjects(dataPath + "/" + "_data_" + s, cl, dbo);
				dbobjs.put(s, dbo);
			} catch (ClassNotFoundException e) {
				log.log(Level.SEVERE, "Cannot instantiate object collection for objects : " + s);
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
				ret[0]="FAIL"; ret[1]="Cannot instantiate objects;";
				return ret;
			}
		}

		if (!props.isProperlyShutdown) { // La BBDD no se ha apagado correctamente asi que
											// debemos aplicar los logs
			log.log(Level.INFO, "Database was not properly shutdown. Recovering from logs, if any");
			try {
				FileReader fr = new FileReader(dataPath + "/" + dbname + "_logging");
				JsonArray array = new JsonParser().parse(fr).getAsJsonArray();
				log.log(Level.INFO, "Updating the Master database with " + array.size() + " logged operations");
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
				fr.close();
			} catch (JsonIOException | JsonSyntaxException | ClassNotFoundException e) {
				System.err.println("No se ha podido aplicar el log a la base de datos " + dbname);
				log.log(Level.SEVERE, "Cannot apply log to database " + dbname);
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
				ret[0] = "FAIL"; ret[1] = "Cannot apply log to databse ... Corruption???";
				return ret;
			} catch (IOException e) {
				// El fichero de logging no existe, una de dos ... o es la primera que vez que se accede
				// y no han habido operaciones de I/O ... o la ultima vez se cerr� correctamente
				//y entonces no deber�amos estar en este punto (isProperlyShutdown == true
				log.log(Level.WARNING, "Master trying to open non-existant logging file. Double check unless this is the first time you open the database");
				ret[1] = dbname + " WARNING Check if this is the first time you open the database!!";
			}
				
		}

		dSyncer.addDatabase(dbname, dataPath + "/");
		props.isProperlyShutdown = false;
		updateProps();
		log.log(Level.INFO, "Master database " + dbname + " opened with " + dbobjs.size() + " objects");
		for (String s : dbobjs.keySet()) {
			log.log(Level.INFO, "Object collection: " + s + " contains " + dbobjs.get(s).size() + " objects");
		}
		return ret;
	}

	@Override
	public String[] close() {
		String[] ret = new String[3];
		ret[0] = "OK";
		String[] temp;
		for (DBObject dbo: dbobjs.values()) {
			temp = objectFlush(dbo);
			if (!temp[0].equals("OK"))
				ret[0] = temp[0];
		}

		for (DBObject o : dbobjs.values())
			o.close();
		dbobjs = null;
		log.log(Level.INFO, "Closing database: " + dbname);

		if (ret[0].equals("OK")) { // All objects were cleanly closed (saved on disk files)
			if (!dSyncer.logOps.isEmpty()) //Quedan elementos en la cola de log no replicados
				dSyncer.netSync();
			dSyncer.logOps.clear();
			Path path = Paths.get(dataPath + "/" + dbname + "_logging");
			if (Files.isRegularFile(path)) {
				try {
					Files.delete(path);
					log.log(Level.INFO, "Cleanly removed logging file for database " + dbname);
					props.isProperlyShutdown = true;
					props.lastProperlyShutdown = new Date();
					updateProps();
					ret[1] = "Database closed";
				} catch (IOException e) {
					log.log(Level.SEVERE, "Cannot delete logging file for database " + dbname);
					log.log(Level.SEVERE, e.getMessage());
					log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
				}
			}
		}
		return ret;
	}


	@Override
	public synchronized String[] insert(String objectName, Object object, boolean logging) {
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
			Class<?> spcl = cl.getSuperclass();
			f = spcl.getDeclaredField("id");
			id = (String) f.get(o);
			f = spcl.getDeclaredField("onDisk");
			f.set(o, !logging);
		} catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
			log.log(Level.WARNING, "Something went wrong trying to insert object of class "+cl.getSimpleName()+" into database "+ dbname);
			ret[0] = "FAIL";
			ret[1] = "Something went wrong with the object to insert in the Database";
			log.log(Level.WARNING, Arrays.toString(e.getStackTrace()));
			return ret;
		}

		dbobjs.get(objectName).insert(id, o);

		if (logging && dSyncer != null)
			dSyncer.enQueue("insert", dbname, objectName, id, o);
		ret[0] = "OK";
		ret[1] = "Inserted new " + objectName + " with id " + id;
		return ret;
	}

	@Override
	public synchronized String[] remove(String objectName, String id, boolean logging) {
		String[] ret = new String[3];
		ret[0] = "FAIL";
		ret[1] = "Object does not exist";
		if (dbobjs.get(objectName).getById(id) != null) {
			dbobjs.get(objectName).remove(id);
			if (logging && dSyncer != null)
				dSyncer.enQueue("remove", dbname, objectName, id, null);
			ret[0] = "OK";
			ret[1] = "Remove object with id " + id;
		}
		return ret;
	}
	
	@Override
	public String[] getMasterInfo() {
		String[] ret = new String[2];
		ret[0] = dataPath;
		ret[1] = String.valueOf(dSyncer.waitTime);
		return ret;
	}

	private String[] loadObjects(String dataFile, Class<?> cl, DBObject dbo) {
		String[] ret = new String[3];
		java.lang.reflect.Type dataType = TypeToken.getParameterized(HashMap.class, String.class, cl).getType();

		try {
			String content = new String(Files.readAllBytes(Paths.get(dataFile)));
			if (!checkFile(cl, signFile(content))) {
				System.err.println("WARNING !! Data file do not match signature");
				log.log(Level.WARNING, "WARNING !! Data file do not match signature");
			}
			dbo.obj = new Gson().fromJson(content, dataType);
		} catch (JsonSyntaxException | JsonIOException e) {
			log.log(Level.WARNING, "WARNING !! Malformed Json file");
			log.log(Level.WARNING, Arrays.toString(e.getStackTrace()));
		} catch (IOException e) {
			// El fichero de datos no existe, asi que asumimos que hay que crearlo
			log.log(Level.INFO, "Master opening non-exixtent datafile. Assuming new Object collection");
		}
		return ret;
	}

	private String[] objectFlush(DBObject dbo) {
		String[] ret = new String[2];
		Gson json = new GsonBuilder().setPrettyPrinting().create();
		Class<?> cl = dbo.cl;
		java.lang.reflect.Type dataType = TypeToken.getParameterized(HashMap.class, String.class, cl).getType();
		String dataFile = dataPath + "/" + "_data_" + cl.getSimpleName();

		try {
			// Make all the onDisk flags true
			Field onDisk = null;
			try {
				for (Object o : dbo.obj.values()) {
					onDisk = o.getClass().getField("onDisk");
					onDisk.set(o, true);
				}
			} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
			}

			String content = json.toJson(dbo.obj, dataType);
			String signature = json.toJson(new Signature(content));
			String signatureFile = dataFile.replace("_data_", "_signature_");
			File f = new File(dataFile);
			f.getParentFile().mkdirs();
			FileWriter fw = new FileWriter(f, false); // Do not append. Overwrite the file if any
			fw.write(content);
			fw.flush();
			fw.close();
			f = new File(signatureFile);
			f.getParentFile().mkdirs();
			fw = new FileWriter(f, false); // Do not append. Overwrite the file if any
			fw.write(signature);
			fw.flush();
			fw.close();
			log.log(Level.INFO, "Object collection" + cl.getName()+ "was cleanly dumnped to disk into file: "+dataFile);
			ret[0] = "OK";
			ret[1] = "";
		} catch (IOException e) {
			ret[0] = "FAIL";
			ret[1] = "Cannot save the datafile";
			log.log(Level.SEVERE, "Cannot write datafile on disk");
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
			e.printStackTrace();
		}
		return ret;
	}
	
	private String signFile(String content) {
		String ret = "";
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hash = digest.digest(content.getBytes("UTF8"));
			ret = new String(Base64.getEncoder().encode(hash));
		} catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			System.err.println("Invalid algorithm. This should never happen");
			log.log(Level.SEVERE, "Cannnot sign files. Invalid algorithm. This should never happen");
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
		}
		return ret;
	}

	private  boolean checkFile(Class<?> cl, String newSign) {
		String storedSign = "";
		String dataFile = dataPath + "/" + "_data_" + cl.getSimpleName();
		try {
			storedSign = ((Signature) new Gson().fromJson(new FileReader(dataFile.replace("_data_", "_signature_")), Signature.class)).signature;
		} catch (JsonSyntaxException | JsonIOException | FileNotFoundException e) {
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
		}
		return (newSign.equals(storedSign));
	}
	
	protected class Signature {
		Date date;
		long timeStamp;
		String signature;

		public Signature(String content) {
			this.date = new Date();
			this.timeStamp = System.currentTimeMillis();
			this.signature = signFile(content);
		}
	}

	@Override
	public String[] replicaUpdateInsert(String objectName, Object object) {
		// Don't do anything as this should never be invoked
		return null;
	}

	@Override
	public String[] replicaUpdateRemove(String objectName, String id) {
		// Don't do anything as this should never be invoked
		return null;
	}
}
