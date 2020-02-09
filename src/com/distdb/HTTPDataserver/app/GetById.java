package com.distdb.HTTPDataserver.app;

import java.util.HashMap;
import java.util.Map;

import com.distdb.dbserver.Database;
import com.google.gson.Gson;

public class GetById extends MiniServlet {

	public String[] doPost(Map<String, Database> dbs, String dbname, String body) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		Datain din = (Datain) new Gson().fromJson(body, Datain.class);
		ret[1] = dbs.get(dbname).getById(din.objectName, din.id);
		
		return ret;
	}
	
	private class Datain {
		String user;
		String token;
		String objectName;
		String id;
	}
}
