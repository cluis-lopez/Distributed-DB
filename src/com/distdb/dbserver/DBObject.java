package com.distdb.dbserver;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.reflect.TypeToken;

public class DBObject {

	protected Map<String, Object> obj;
	protected Class<?> cl;
	Logger log;
	java.lang.reflect.Type dataType;

	public DBObject( Class<?> cl, Logger log) {
		this.cl = cl;
		this.log = log;
		dataType = TypeToken.getParameterized(HashMap.class, String.class, cl).getType();
		obj = new HashMap<>();
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
}
