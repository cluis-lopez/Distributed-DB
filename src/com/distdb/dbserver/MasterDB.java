package com.distdb.dbserver;

import java.util.logging.Logger;

public class MasterDB implements Runnable {
	
	Logger log;
	int port;
	
	public MasterDB (Logger log, int clusterPort) {
		port = clusterPort;
	}

	@Override
	public void run() {

	}
}
