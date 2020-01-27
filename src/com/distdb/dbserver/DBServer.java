package com.distdb.dbserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.distdb.dbserver.DistServer.Type;

public class DBServer {
	
		private Logger log;
		private List<Database> dbs = new ArrayList();
		
		public DBServer(Logger log, List<Map<String,String>> databases, Type type) {
			this.log = log;
			for (Map<String, String> m: databases) {
				Database db = new Database(log, m.get("Name"), m.get("DefFile"), m.get("defPath"), type);
				dbs.add(db);
			}
		}
}
