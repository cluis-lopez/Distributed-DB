package com.distdb.HTTPDataserver.app;

import java.util.Map;
import com.distdb.dbserver.Database;
import com.distdb.dbserver.DistServer.DBType;
import com.distdb.dbsync.DiskSyncer;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class AdminServer extends MiniServlet {
	public String[] doPost(Map<String, Database> dbs, String dbname, String body) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		ret[1] = new Gson().toJson("Invalid database management command");
		Datain din = new Gson().fromJson(body, Datain.class);
		
		if (din.command.equals("shutdown")) {
			String[] result = new String[2];
			result = dbs.get(dbname).close();
			if (result[0].equals("OK"))
					dbs.remove(dbname);
			ret [1] = new Gson().toJson(result);
		}
		
		if (din.command.equals("startup")) {
			if (dbs.get(dbname) != null) {
				ret[1] = "Database already running";
			} else {
				String configFile = din.payload.get("configFile").getAsString();
				String defPath = din.payload.get("defPath").getAsString();
				String dbt = din.payload.get("DBType").getAsString();
				DBType dbtype = DBType.REPLICA;
				if (dbt.equals("Master") || dbt.equals("MASTER"))
						dbtype = DBType.MASTER;
				dbs.put(dbname, new Database(log, dbname, configFile, defPath, null, dbtype));
			}
		}
		
		if (din.command.equals("dbInfo")) {
			ret[1] = dbs.get(dbname).getInfo();
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
