package com.distdb.dbserver;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.util.Date;
import java.util.Scanner;
import java.util.logging.Logger;

import com.distdb.HttpHelpers.HTTPDataMovers;
import com.distdb.HttpHelpers.HelperJson;
import com.distdb.dbserver.DistServer.DBType;

/**
 * @author clopezherna4
 *
 */
public class Node {
	private Logger log;
	public Node me;
	public String name;
	public URL url;
	public DBType dbtype;
	public boolean isLive;
	public long lastReached;
	public Date lastUpdated;
	public int ticksSinceLastSeen;

	public Node(Logger log, String name, URL url, DBType dbtype) {
		this.name = name;
		this.url = url;
		this.dbtype = dbtype;
		this.isLive = false;
		this.ticksSinceLastSeen = 0;
	}

	/**
	 * Check if the node is reachable by Java standards (ICMP) in the predefinided 3
	 * seconds timeout
	 * 
	 * @return True if the node is reachable
	 */
	public boolean isReachable() {
		String hostname = url.getHost();
		boolean ret = false;
		try {
			InetAddress ip = InetAddress.getByName(hostname);
			if (ret = ip.isReachable(3000)) {
				this.isLive = true;
				this.lastReached = System.currentTimeMillis();
				ret = true;
			}
		} catch (IOException e) {
			ret = false;
			this.isLive = false;
		}
		return ret;
	}

	public boolean isAlive() {
		String responseBody = HTTPDataMovers.postData(log, url, "", "ping", "{'user':'', 'token':''}");
		if (responseBody.equals(""))
			return false;
		if (HelperJson.decodeCodes(responseBody)[0].equals("OK"))
				return true;;
		return false;
	}
	
	public String[] fullCheck() {
		String[] ret = new String[2];
		System.err.println("Checking if node "+this.name+" is reachable ...");
		if (! this.isReachable()) {
			ret[0] = "FAIL"; ret[1] ="Node is not reachable";
			System.err.println("Node not reachable");
			return ret;
		}
		System.err.println("Checking if the node has DistServer cluster service running at "+ this.url);
		if (! this.isAlive()) {
			ret[0] = "FAIL"; ret[1] ="Node has not running cluster service at "+ this.url;
			return ret;
		}
		ret[0] = "OK"; ret[1] = "";
		return ret;
	}
}
