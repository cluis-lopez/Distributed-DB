package com.distdb.dbsync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DiskSyncer implements Runnable {
	
	List<LoggedObj> addQueue; //LIFO queue to write changes on disk
	int waitTime;

	public DiskSyncer(int waitTime) {
		addQueue = new ArrayList<>();
		this.waitTime = waitTime;
	}

	public void addObject(String database,String objectName, String id, Object o) {
		addQueue.add(new LoggedObj(database, objectName, id, o));
	}
	
	
	@Override
	public void run() {
		while (true) { //NOSONAR
			if (addQueue.size() == 0)
				break;
			for (int i = 0; i<addQueue.size(); i++) {

			}
		}
		
	}
	
	private void appendJson() {
		
	}
	
	private class LoggedObj{
		String database;
		String objectName;
		String id;
		Object o;
		
		public LoggedObj(String database, String objectName, String id, Object o) {
			this.database = database;
			this.objectName = objectName;
			this.id = id;
			this.o = o;
		}
	}
}
