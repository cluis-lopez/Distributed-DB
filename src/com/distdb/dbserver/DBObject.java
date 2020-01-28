package com.distdb.dbserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbserver.DistServer.Type;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class DBObject<cl> {

	private Map<String, Object> obj;
	private Class<?> cl;
	private String dataFile;
	Logger log;
	Type type;

	public DBObject(String dataFile, Class cl, Type type, Logger log) {

		this.cl = cl;
		this.log = log;
		this.type = type;
		this.dataFile = dataFile;

		try {
			obj = (Map<String, Object>) new Gson().fromJson(new FileReader(dataFile), HashMap.class);
		} catch (JsonSyntaxException | JsonIOException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// El fichero de datos no existe, asi que asumimos que hay que crearlo
			if (type == Type.MASTER) {
				obj = new HashMap<>();
				System.err.println("No existe el fichero de datos");
				log.log(Level.INFO, "Master replica opneing non-exixtent datafile. Assuming new Object collection");
			} else { //Replica database. Should update from a master node
				
			}
		}

	}
	
	public String[] flush() {
			String [] ret = new String[2];
			Gson json = new GsonBuilder().setPrettyPrinting().create();
			try {
				File f = new File(dataFile);
				f.getParentFile().mkdirs();
				FileWriter fw = new FileWriter(f, false); //Not to append. Overwrite the file if any
				fw.write(json.toJson(obj));
				fw.flush();
				fw.close();
				ret[0] = "OK"; ret[1] ="";
			} catch (IOException e) {
				ret[0] = "FAIL"; ret[1] = "Cannot save the datafile";
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
				log.log(Level.INFO, "Searching on non-string fields is not implemented. The field " + f.getName()
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
}
