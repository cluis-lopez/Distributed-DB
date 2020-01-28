package com.distdb.dbsync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DiskSyncer implements Runnable {
	
	Map<String, Object> queue;
	List<Map<String, Object>> queues; //LIFO queue to write changes on disk

	public DiskSyncer() {
		queue = new HashMap<>();
		queues = new ArrayList<>();
	}
}
