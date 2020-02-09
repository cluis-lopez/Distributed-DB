package com.distdb.HTTPDataserver.app;

import java.util.Map;
import com.distdb.dbserver.Database;
import com.google.gson.Gson;

public class AdminServer extends MiniServlet {
	public String[] doPost(Map<String, Database> dbs, String dbname, String body) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		Datain din = new Gson().fromJson(body, Datain.class);
		
		if (din.command.equals("shutdown")) {
			String[] result = new String[2];
			result = dbs.get(dbname).close();
			ret [1] = new Gson().toJson(result);
		}
		
		
		
		
		return ret;
	}
	
	private class Datain {
		String user;
		String token;
		String command;
	}
}
