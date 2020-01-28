package com.distdb.dbsync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DiskSyncer implements Runnable {
	
	AddQueue add;
	List<AddQueue> addQueue; //LIFO queue to write changes on disk
	int waitTime;

	public DiskSyncer(int waitTime) {
		add = new AddQueue();
		addQueue = new ArrayList<>();
		this.waitTime = waitTime;
	}

	public void addObject(String database, String id, Object o) {
		
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
	
	private class AddQueue{
		String database;
		String objectName;
		String id;
		Object o;
	}
}
