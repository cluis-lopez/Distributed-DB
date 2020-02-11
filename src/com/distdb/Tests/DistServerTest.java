package com.distdb.Tests;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DistServerTest {

		public static final String url = "http://localhost:8080";
		
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@Test
	void testDistServer() {
		fail("Not yet implemented");
	}
	
	private String sendData(String database, String service, String postData) {
		String ret = "";
		
		URL uri;
		try {
			uri = new URL(url + "/"+database+"/"+service);
			HttpURLConnection con = (HttpURLConnection) uri.openConnection();
			con.setRequestMethod("POST");
			con.setRequestProperty("Content-Type", "application/json");
			
			con.setConnectTimeout(5000);
			con.setReadTimeout(5000);
		} catch ( IOException e) {
			e.printStackTrace();
		}
		
	
		return ret;
	}

}
