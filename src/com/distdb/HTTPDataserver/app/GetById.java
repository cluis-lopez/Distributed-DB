package com.distdb.HTTPDataserver.app;

import java.util.Map;

import com.distdb.HttpHelpers.HelperJson;
import com.distdb.dbserver.MasterDatabase;
import com.distdb.dbsync.MasterSyncer;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class GetById extends MiniServlet {

	public String[] doPost(Map<String, MasterDatabase> dbs, String dbname, String body, MasterSyncer dsync) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		Datain din = null;
		try {
			din = (Datain) new Gson().fromJson(body, Datain.class);
		} catch (JsonIOException | JsonSyntaxException e) {
			ret[1] = HelperJson.returnCodes("FAIL", "Invalid Json request", "");
			return ret;
		}
		
		String[] temp = dbs.get(dbname).getById(din.objectName, din.id);
		ret[1] = HelperJson.returnCodes(temp[0],  temp[1],  temp[2]);
		return ret;
	}

	private class Datain {
		String user;
		String token;
		String objectName;
		String id;
	}
}
