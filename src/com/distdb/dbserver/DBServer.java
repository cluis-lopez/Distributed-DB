package com.distdb.dbserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.distdb.dbserver.DistServer.DBType;
import com.distdb.dbsync.DiskSyncer;

public class DBServer {
	
		private Logger log;
		private List<Database> dbs = new ArrayList();
		private DiskSyncer dSyncer = null;
		
		public DBServer(Logger log, List<Map<String,String>> databases, DBType type) {
			this.log = log;
			if (type == DBType.MASTER) {
				dSyncer = new DiskSyncer(1000*60*5); //Sync every 5 minutes
			}
			
			for (Map<String, String> m: databases) {
				Database db = new Database(log, m.get("Name"), m.get("DefFile"), m.get("defPath"), dSyncer, type);
				dbs.add(db);
			}
		}
}
