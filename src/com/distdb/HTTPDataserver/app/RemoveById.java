package com.distdb.HTTPDataserver.app;

import java.util.Map;

import com.distdb.dbserver.Database;
import com.google.gson.Gson;

public class RemoveById extends MiniServlet {

	public String[] doPost(Map<String, Database> dbs, String dbname, String body) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		Datain din = new Gson().fromJson(body, Datain.class);
		String[] temp = dbs.get(dbname).remove(din.objectName, din.id);

		ret[1] = new Gson().toJson(temp[1]);

		return ret;
	}

	private class Datain {
		String user;
		String token;
		String objectName;
		String id;
	}

}
