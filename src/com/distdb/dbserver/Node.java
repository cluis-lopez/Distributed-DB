package com.distdb.dbserver;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.util.Date;
import java.util.Scanner;

import com.distdb.HttpHelpers.HelperJson;
import com.distdb.dbserver.DistServer.DBType;

/**
 * @author clopezherna4
 *
 */
public class Node {
	public Node me;
	public String name;
	public URL url;
	public DBType dbtype;
	public boolean isAlive;
	public long lastReached;
	public Date lastUpdated;

	public Node(String name, URL url, DBType dbtype) {
		this.name = name;
		this.url = url;
		this.dbtype = dbtype;
		this.isAlive = false;
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
		;
		try {
			InetAddress ip = InetAddress.getByName(hostname);
			if (ret = ip.isReachable(3000)) {
				this.isAlive = true;
				this.lastReached = System.currentTimeMillis();
			}
		} catch (IOException e) {
			ret = false;
			this.isAlive = false;
		}
		return ret;
	}

	public boolean isAlive() {
		boolean ret = false;
		try {
			URL url2 = new URL(url.toString() + "/ping");
			InputStream response = url2.openStream();
			Scanner scanner = new Scanner(response);
			String responseBody = scanner.useDelimiter("\\A").next();
			if (HelperJson.decodeCodes(responseBody)[0].equals("OK"))
				ret = true;
			scanner.close();
			response.close();
		} catch (IOException e) {
			ret = false;
		}
		return ret;
	}
	
	public String[] fullCheck() {
		String[] ret = new String[2];
		System.err.println("Checking if node "+this.name+" is reachable ...");
		if (! this.isReachable()) {
			ret[0] = "FAIL"; ret[1] ="Node is not reachable";
			System.err.println("Node not reachable");
			return ret;
		}
		System.err.println("Checking if the node has DistServer cluster service running at ."+ this.url);
		if (! this.isAlive()) {
			ret[0] = "FAIL"; ret[1] ="Node has not running cluster service at "+ this.url;
			return ret;
		}
		ret[0] = "OK"; ret[1] = "";
		return ret;
	}
}
