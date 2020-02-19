package com.distdb.HTTPDataserver.app;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import com.distdb.HttpHelpers.HelperJson;
import com.distdb.dbserver.MasterDatabase;
import com.distdb.dbsync.MasterSyncer;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class Insert extends MiniServlet {

	public String[] doPost(Map<String, MasterDatabase> dbs, String dbname, String body, MasterSyncer dsync) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		InsertDatain din = null;
		try {
			din = new Gson().fromJson(body, InsertDatain.class);
		} catch (JsonIOException | JsonSyntaxException e) {
			ret[1] = HelperJson.returnCodes("FAIL", "Invalid Json request", "");
			return ret;
		}
		
		try {
			Class<?> cl = Class.forName(dbs.get(dbname).defPath + "." + din.objectName);
			Constructor<?>[] cons = cl.getConstructors();
			if (cons == null || cons.length == 0 || cons.length>1) {
				ret[1] = HelperJson.returnCodes("FAIL", "Database objects must have a single constructor while "+din.objectName+" has "+cons.length, "");
				return ret;
			}
			
			Class<?>[] consArgs = cons[0].getParameterTypes();
			
			if (din.args.size() != consArgs.length) {
				ret[1] = HelperJson.returnCodes("FAIL","Number of arguments mismatch. Used: " + din.args.size() + " while object " + din.objectName
						+ "constructor, expects " + consArgs.length, "");
				return ret;
			}
			
			Object[] params = new Object[consArgs.length];
			for(int i= 0; i < consArgs.length; i++) {
				params[i] = toObject(consArgs[i], din.args.get(i));
			}
			
			Object toInsert = cons[0].newInstance(params);
			toInsert = cl.cast(toInsert);
			String[] temp = (dbs.get(dbname).insert(din.objectName, toInsert));
			ret[1] = HelperJson.returnCodes(temp[0], temp[1],  "");
		} catch (ClassNotFoundException e) {
			log.log(Level.INFO, "Invalid object to insert");
			log.log(Level.INFO, e.getMessage());
		} catch (InstantiationException | IllegalAccessException e){
			log.log(Level.SEVERE, "Cannot instantiate command/servlet");
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
		} catch (InvocationTargetException e) {
			log.log(Level.SEVERE, "Cannot instantiate the object to insert");
			log.log(Level.SEVERE, e.getMessage());
			log.log(Level.SEVERE, Arrays.toString(e.getCause().getStackTrace()));
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
		}
		return ret;
	}
	
	public class InsertDatain {
		String user;
		String token;
		String objectName;
		List<String> args;
	}
	
	private static Object toObject( Class clazz, String value ) {
		if( ! clazz.isPrimitive()) {
		    if( Boolean.class == clazz ) return Boolean.parseBoolean( value );
		    if( Byte.class == clazz ) return Byte.parseByte( value );
		    if( Short.class == clazz ) return Short.parseShort( value );
		    if( Integer.class == clazz ) return Integer.parseInt( value );
		    if( Long.class == clazz ) return Long.parseLong( value );
		    if( Float.class == clazz ) return Float.parseFloat( value );
		    if( Double.class == clazz ) return Double.parseDouble( value );
		    return value;
		} else { //The parameters is a primitive
			if(clazz.getTypeName().equals("boolean")) return Boolean.parseBoolean(value);
			if(clazz.getTypeName().equals("byte")) return Byte.parseByte(value);
			if(clazz.getTypeName().equals("short")) return Short.parseShort(value);
			if(clazz.getTypeName().equals("int")) return Integer.parseInt(value);
			if(clazz.getTypeName().equals("long")) return Long.parseLong(value);
			if(clazz.getTypeName().equals("float")) return Float.parseFloat( value );
		    if(clazz.getTypeName().equals("double")) return Double.parseDouble( value );
		}
		return value;
	}
}
