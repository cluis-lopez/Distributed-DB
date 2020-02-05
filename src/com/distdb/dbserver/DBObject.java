package com.distdb.dbserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbserver.DistServer.DBType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public class DBObject<cl> {

	private Map<String, Object> obj;
	private Class<?> cl;
	private String dataFile;
	Logger log;
	DBType type;
	java.lang.reflect.Type dataType;

	public DBObject(String dataFile, Class cl, DBType type, Logger log) {

		this.cl = cl;
		this.log = log;
		this.type = type;
		this.dataFile = dataFile;

		dataType = TypeToken.getParameterized(HashMap.class, String.class, cl).getType();

		try {
			String content = new String(Files.readAllBytes(Paths.get(dataFile)));
			if (!checkFile(signFile(content))) {
				System.err.println("WARNING !! Data file do not match signature");
				log.log(Level.WARNING, "WARNING !! Data file do not match signature");
			}
			obj = new Gson().fromJson(content, dataType);
		} catch (JsonSyntaxException | JsonIOException e) {
			log.log(Level.WARNING, "WARNING !! Malformed Json file");
			log.log(Level.WARNING, Arrays.toString(e.getStackTrace()));
		} catch (IOException e) {
			// El fichero de datos no existe, asi que asumimos que hay que crearlo
			if (type == DBType.MASTER) {
				obj = new HashMap<>();
				System.err.println("No existe el fichero de datos");
				log.log(Level.INFO, "Master opening non-exixtent datafile. Assuming new Object collection");
			} else { // Replica database. Should update from a master node

			}
		}
	}

	public String[] flush() {
		String[] ret = new String[2];
		Gson json = new GsonBuilder().setPrettyPrinting().create();
		try {
			// Make all the onDisk flags true
			Field onDisk = null;
			try {
				for (Object o : obj.values()) {
					onDisk = o.getClass().getField("onDisk");
					onDisk.set(o, true);
				}
			} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
				log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
			}

			String content = json.toJson(obj, dataType);
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
			System.err.println("Salvados los ficheros de la colección de objetos " + cl.getName());
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

	public void insert(String id, Object o) {
		obj.put(id, o);
	}

	public void remove(String id) {
		obj.remove(id);
	}

	public Object getById(String id) {
		return obj.get(id);
	}

	public List<Object> searchByField(String fieldName, String value) {
		List<Object> ret = new ArrayList<>();
		try {
			Field f = cl.getField(fieldName);
			if (!f.getType().getName().equals("java.lang.String")) {
				log.log(Level.INFO, "Searching on non-string fields is not yet implemented. The field " + f.getName()
						+ " is of type " + f.getType().getName());
				return ret;
			}
			for (Object o : obj.values()) {
				if (((String) f.get(o)).contains(value))
					ret.add(o);
			}
		} catch (NoSuchFieldException | SecurityException | IllegalAccessException | IllegalArgumentException e) {
			log.log(Level.INFO, "No such field. Returning empty dataset");
		}
		return ret;
	}

	public boolean isObject(String id) {
		return obj.containsKey(id);
	}

	public int size() {
		return obj.size();
	}

	public void close() {
		obj = null;
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

	private boolean checkFile(String newSign) {
		String storedSign = "";
		try {
			storedSign = ((Signature) new Gson().fromJson(new FileReader(dataFile.replace("_data_", "_signature_")),
					Signature.class)).signature;
		} catch (JsonSyntaxException | JsonIOException | FileNotFoundException e) {
			e.printStackTrace();
		}
		return (newSign.equals(storedSign));
	}

	private class Signature {
		Date date;
		String signature;

		public Signature(String content) {
			this.date = new Date();
			this.signature = signFile(content);
		}
	}
}
