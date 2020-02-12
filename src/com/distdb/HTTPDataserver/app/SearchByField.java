package com.distdb.HTTPDataserver.app;

import java.util.Map;

import com.distdb.dbserver.Database;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class SearchByField extends MiniServlet {
	
	public String[] doPost(Map<String, Database> dbs, String dbname, String body) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		Datain din = null;
		try {
			din = (Datain) new Gson().fromJson(body, Datain.class);
		} catch (JsonSyntaxException e) {
			ret[1] = "Invalid Json request";
			return ret;
		}
		
		String[] temp = dbs.get(dbname).searchByField(din.objectName,  din.fieldName,  din.value);
		if (!temp[0].equals("OK"))
			ret[1] = temp[1];
		else {
			ret[1] = temp[2];
		}
		return ret;
	}
	
	private class Datain {
		String user;
		String token;
		String objectName;
		String fieldName;
		String value;
	}

}
