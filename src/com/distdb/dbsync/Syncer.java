package com.distdb.dbsync;

import java.util.List;
import java.util.Map;

public class Syncer {
	
	Map<String, Object> queue;
	List<Map<String, Object>> queues; //LIFO queue to write changes on disk

}
