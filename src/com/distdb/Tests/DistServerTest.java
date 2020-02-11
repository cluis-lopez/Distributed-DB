package com.distdb.Tests;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.distdb.TestDB1.User;
import com.distdb.dbserver.DistServer;
import com.google.gson.Gson;

class DistServerTest {

	public static final String url = "http://localhost:8080";

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		//DistServer ds = new DistServer();
		//Assume you have to startup the server outside
	}

	@Test
	void testDistServer() {
		String postData = "{'user': '', 'token': '',  'objectName': 'User', 'id': 'c480eb2d-6a67-4066-a07f-3a906bc60b89'}";
		String db = "TestDB1";
		String ret = sendData(db, "GetById", postData);
		System.err.println(ret);
		User u1 = new Gson().fromJson(ret, User.class);
		Assertions.assertEquals("nuevoclopez", u1.name);
	}

	private String sendData(String database, String service, String postData) {
		String ret = "";
		URL uri;
		try {
			uri = new URL(url + "/" + database + "/" + service);
			HttpURLConnection con = (HttpURLConnection) uri.openConnection();
			con.setRequestMethod("POST");
			con.setRequestProperty("Content-Type", "application/json");
			con.setRequestProperty("Content-length", String.valueOf(postData.length()));
			con.setUseCaches(false);
			con.setDoOutput(true);
			con.setConnectTimeout(5000);
			con.setReadTimeout(5000);

			DataOutputStream output = new DataOutputStream(con.getOutputStream());
			output.writeBytes(postData);
			output.close();

			// "Post data send ... waiting for reply");
			int code = con.getResponseCode(); // 200 = HTTP_OK

			// read the response
			if (code == 200) {
				DataInputStream input = new DataInputStream(con.getInputStream());
				int c;
				StringBuilder resultBuf = new StringBuilder();
				while ((c = input.read()) != -1) {
					resultBuf.append((char) c);
				}
				ret = resultBuf.toString();
				input.close();
			} else {
				System.out.println("Response    (Code):" + code);
				System.out.println("Response (Message):" + con.getResponseMessage());
				ret = code + ":" +con.getResponseMessage();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ret;
	}

}
