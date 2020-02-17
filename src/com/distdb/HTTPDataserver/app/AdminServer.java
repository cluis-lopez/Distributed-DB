package com.distdb.HTTPDataserver.app;

import java.util.Map;
import com.distdb.dbserver.MasterDatabase;
import com.distdb.dbserver.DistServer.DBType;
import com.distdb.dbsync.DiskSyncer;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

public class AdminServer extends MiniServlet {
	public String[] doPost(Map<String, MasterDatabase> dbs, String dbname, String body, DiskSyncer dSync) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		ret[1] =HelperJson.returnCodes("FAIL",  "Invalid management command", "");
		
		try {
			Datain din = new Gson().fromJson(body, Datain.class);
			
			if (din.command.equals("shutdown")) {
				String[] result = new String[2];
				result = dbs.get(dbname).close();
				if (result[0].equals("OK"))
						dbs.remove(dbname);
				ret [1] = HelperJson.returnCodes(result[0], result[1], "");
			}
			
			if (din.command.equals("startup")) {
				if (dbs.get(dbname) != null) {
					ret[1] = HelperJson.returnCodes("FAIL", "Database already loaded", "");
				} else {
					String configFile = din.payload.get("configFile").getAsString();
					String defPath = din.payload.get("defPath").getAsString();
					String dbt = din.payload.get("DBType").getAsString();
					if (dbt.equals("Master") || dbt.equals("MASTER")) {
						dbs.put(dbname, new MasterDatabase(log, dbname, configFile, defPath, dSync));
						dbs.get(dbname).open();
					}
					ret[1] = HelperJson.returnCodes("OK", dbname + "Database started", "");
				}
			}
			
			if (din.command.equals("dbInfo")) {
				if (dbs.containsKey(dbname))
					ret[1] = HelperJson.returnCodes("OK",  "",  dbs.get(dbname).getInfo());
				else
					ret[1] = HelperJson.returnCodes("FAIL",  "Database not running",  "");
			}
			
		} catch (JsonIOException | JsonSyntaxException e) {
			ret[1] = HelperJson.returnCodes("FAIL",  "Invalid Json payload", "");
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
