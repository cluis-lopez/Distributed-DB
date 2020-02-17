package com.distdb.HTTPDataserver.app;

import java.util.Map;

import com.distdb.dbserver.MasterDatabase;
import com.distdb.dbsync.DiskSyncer;
import com.google.gson.Gson;

public class RemoveById extends MiniServlet {

	public String[] doPost(Map<String, MasterDatabase> dbs, String dbname, String body, DiskSyncer dsync) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		Datain din = new Gson().fromJson(body, Datain.class);
		String[] temp = dbs.get(dbname).remove(din.objectName, din.id);

		ret[1] = HelperJson.returnCodes(temp[0],  temp[1], "");

		return ret;
	}

	private class Datain {
		String user;
		String token;
		String objectName;
		String id;
	}

}
