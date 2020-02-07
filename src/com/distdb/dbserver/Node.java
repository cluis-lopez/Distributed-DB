package com.distdb.dbserver;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.util.Scanner;

import com.distdb.dbserver.DistServer.DBType;

/**
 * @author clopezherna4
 *
 */
public class Node {
	String name;
	URL url;
	DBType dbtype;
	boolean isAlive;

	public Node(String name, URL url, DBType dbtype) {
		this.name = name;
		this.url = url;
		this.dbtype = dbtype;
		this.isAlive = false;
	}

	/** Check if the node is reachable by Java standards (ICMP) in the predefinided 3 seconds timeout
	 * @return True if the node is reachable
	 */
	public boolean isReachable() {
		String hostname = url.getHost();
		boolean ret = false;;
		try {
			InetAddress ip = InetAddress.getByName(hostname);
			ret = ip.isReachable(3000);
		} catch ( IOException e) {
			ret = false;
		}
		return ret;
	}

	public boolean isMasterAlive() {
		boolean ret = false;
		try {
			URL url2 = new URL(url.toString() + "/ping");
			InputStream response = url2.openStream();
			Scanner scanner = new Scanner(response);
			String responseBody = scanner.useDelimiter("\\A").next();
			 if (responseBody.equals("OK"))
				 ret = true;
			 scanner.close();
			 response.close();
		} catch (IOException e) {
			ret = false;
		}
		return ret;
	}
}
