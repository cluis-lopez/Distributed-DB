package com.distdb.dbserver;

import java.net.URL;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbserver.DistServer.DBType;

public class WatchDog implements Runnable {
	private Logger log;
	private int pingTime;
	private int maxTicksDead;
	public EnumMap<DBType, List<Node>> declaredNodes;
	public List<Node> liveReplicas;
	public static AtomicBoolean keepRunning;

	public WatchDog(Logger log, EnumMap<DBType, List<Node>> declaredNodes, List<Node> liveReplicas, int pingTime, int maxTicksDead) {
		this.log = log;
		this.declaredNodes = declaredNodes;
		this.liveReplicas = liveReplicas;
		this.pingTime = pingTime;
		this.maxTicksDead = maxTicksDead;
		keepRunning.set(true);
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
							// Take away this node out of the cluster
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
