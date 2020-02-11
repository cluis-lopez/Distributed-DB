package com.distdb.HTTPDataserver.app;

import java.util.Map;
import com.distdb.dbserver.Database;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class AdminServer extends MiniServlet {
	public String[] doPost(Map<String, Database> dbs, String dbname, String body) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		Datain din = new Gson().fromJson(body, Datain.class);
		
		if (din.command.equals("shutdown")) {
			String[] result = new String[2];
			result = dbs.get(dbname).close();
			if (result[0].equals("OK"))
					dbs.remove(dbname);
			ret [1] = new Gson().toJson(result);
		}
		
		if (din.command.equals("startup")) {
			String[] result = new String[2];
			if (dbs.get(dbname) != null) {
				ret[1] = "Database already running";
			} else {
				din.payload.getAsString();
				dbs.put(dbname, new Database(log, dbname, din.payload.getAsString("configFile"), din.payload.getAsString("defPath"), null, din.payload.getAsString("DBType"));
			}
			
		}
		
		
		
		return ret;
	}
	
	private class Datain {
		String user;
		String token;
		String command;
		JsonObject payload;
	}
}
