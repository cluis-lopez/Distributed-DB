package com.distdb.HTTPDataserver.app;

import java.util.HashMap;
import java.util.Map;

import com.distdb.dbserver.Database;

public class GetById extends MiniServlet {

	public String[] doPost(Map<String, Database> dbs, String dbname, String body) {
		String[] ret = new String[2];
		ret[0] = "application/json";
		dbs.get(ret[1]).getById(objectName, id);
		
		
		return ret;
	}
}
