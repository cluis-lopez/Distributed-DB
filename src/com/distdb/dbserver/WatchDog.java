package com.distdb.dbserver;


import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbserver.DistServer.DBType;

public class WatchDog implements Runnable {
	private Logger log;
	private int pingTime;
	private int maxTicksDead;
	public Map<DBType, List<Node>> declaredNodes;
	public List<Node> liveReplicas;
	public static AtomicBoolean keepRunning;

	public WatchDog(Logger log, Map<DBType, List<Node>> declaredNodes, List<Node> liveReplicas, int pingTime, int maxTicksDead) {
		this.log = log;
		this.declaredNodes = declaredNodes;
		this.liveReplicas = liveReplicas;
		this.pingTime = pingTime;
		this.maxTicksDead = maxTicksDead;
		keepRunning = new AtomicBoolean(true);
	}

	public void kill() {
		keepRunning.set(false);
	}

	@Override
	public void run() {
		while (keepRunning.get()) {
			for (Node n : declaredNodes.get(DBType.REPLICA)) {
				if (n.fullCheck()[0].equals("OK")) {
					n.lastReached = System.currentTimeMillis();
					n.isLive = true;
				} else {
					n.isLive = false;
					if (liveReplicas.contains(n)) { // The replica that does not answer is part of the cluster
						n.ticksSinceLastSeen++;
						if (n.ticksSinceLastSeen > maxTicksDead) {
							//Remove this database from the liveReplicas list
							for (int i = 0; i<liveReplicas.size(); i++) {
								if (liveReplicas.get(i).name.equals(n.name)) {
									
										log.log(Level.INFO, "Replica "+n.name+" is now removed from the cluster");
										liveReplicas.remove(i);
							}
									
							}
						}
					}
				}
			}
			try {
				Thread.sleep(pingTime);
			} catch (InterruptedException e) {
				log.log(Level.SEVERE, "Interrupted exception in thread WatchDog");
			}
		}

	}

}
