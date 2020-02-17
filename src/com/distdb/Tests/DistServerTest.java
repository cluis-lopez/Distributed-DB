package com.distdb.Tests;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

import com.distdb.TestDB1.User;
import com.distdb.dbserver.DistServer;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@TestMethodOrder(OrderAnnotation.class)
class DistServerTest {

	public static final String url = "http://localhost:8080";

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		//DistServer ds = new DistServer();
		//Assume you have to startup the server outside
		//TestDB1 must be empty before starting
	}

	@Test
	@Order(1)
	void testDistServer() {
		String postData = "{'user': '', 'token': '',  'objectName': 'User', 'args': [ 'clopez', 'clopez@gmail.com', '1234']}";
		String db = "TestDB1";
		String ret = sendData(db, "Insert", postData);
		JsonArray ja = new JsonParser().parse(ret).getAsJsonArray();
		Assertions.assertEquals("OK", ja.get(0).getAsString());
		postData = "{'user': '', 'token': '',  'objectName': 'User', 'args': [ 'manolo', 'manolo@hotmail.com', '1234']}";
		ret = sendData(db, "Insert", postData);
		ja = new JsonParser().parse(ret).getAsJsonArray();
		Assertions.assertEquals("OK", ja.get(0).getAsString());
		postData = "{'user': '', 'token': '',  'objectName': 'User', 'args': [ 'jacinto', 'jacinto@hotmail.com', '1234']}";
		ret = sendData(db, "Insert", postData);
		ja = new JsonParser().parse(ret).getAsJsonArray();
		Assertions.assertEquals("OK", ja.get(0).getAsString());
		postData = "{'user': '', 'token': '',  'command': 'dbInfo', 'payload': {}}";
		ret = sendData(db, "AdminServer", postData);
		ja = new JsonParser().parse(ret).getAsJsonArray();
		JsonObject jo = ja.get(2).getAsJsonObject();
		Assertions.assertEquals(3, jo.getAsJsonPrimitive("User").getAsInt());
		postData = "{'user': '', 'token': '',  'objectName': 'User', 'fieldName': 'name', 'value': 'lopez'}";
		ret = sendData(db, "SearchByField", postData);
		ja = new JsonParser().parse(ret).getAsJsonArray();
		JsonArray ja2 = ja.get(2).getAsJsonArray();
		Assertions.assertEquals(1, ja2.size());
		com.distdb.TestDB1.User u1 = (User) new Gson().fromJson(ja2.get(0), User.class);
		Assertions.assertEquals("clopez", u1.name);
		
		postData = "{'user': '', 'token': '',  'objectName': 'Event', 'args': [ 'Evento1', 'Lo del coronavirus', 'en Barcelona']}";
		ret = sendData(db, "Insert", postData);
		postData = "{'user': '', 'token': '',  'objectName': 'Event', 'args': [ 'Evento2', 'Lo de los moviles', 'sine die']}";
		ret = sendData(db, "Insert", postData);
		postData = "{'user': '', 'token': '',  'command': 'dbInfo', 'payload': {}}";
		ret = sendData(db, "AdminServer", postData);
		ja = new JsonParser().parse(ret).getAsJsonArray();
		jo = ja.get(2).getAsJsonObject();
		Assertions.assertEquals(2, jo.getAsJsonPrimitive("Event").getAsInt());
		
		//3 "User" and 2 "Events" in the database
		// Close and restart
		try {
			Thread.sleep(40000); //Wait 40 seconds for the syncer to write on disk
		} catch (InterruptedException e) {
			
		}
		postData = "{'user': '', 'token': '',  'command': 'shutdown', 'payload': {}}";
		ret = sendData(db, "AdminServer", postData);
		ja = new JsonParser().parse(ret).getAsJsonArray();
		Assertions.assertEquals("OK", ja.get(0).getAsString());
		Assertions.assertEquals("Database closed", ja.get(1).getAsString());
		postData = "{'user': '', 'token': '',  'command': 'startup', 'payload': {'configFile': 'TestDB1.json', 'defPath': 'com.distdb.TestDB1', 'DBType': 'Master'}}";
		ret = sendData(db, "AdminServer", postData);
		ja = new JsonParser().parse(ret).getAsJsonArray();
		Assertions.assertEquals("OK", ja.get(0).getAsString());
		postData = "{'user': '', 'token': '',  'command': 'dbInfo', 'payload': {}}";
		ret = sendData(db, "AdminServer", postData);
		ja = new JsonParser().parse(ret).getAsJsonArray();
		jo = ja.get(2).getAsJsonObject();
		Assertions.assertEquals(2, jo.getAsJsonPrimitive("Event").getAsInt());
		postData = "{'user': '', 'token': '',  'command': 'dbInfo', 'payload': {}}";
		ret = sendData(db, "AdminServer", postData);
		ja = new JsonParser().parse(ret).getAsJsonArray();
		jo = ja.get(2).getAsJsonObject();
		Assertions.assertEquals(3, jo.getAsJsonPrimitive("User").getAsInt());
		
	}
	
	@Test
	@Order(2)
	void testGETServer() {
		
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
				ret = code + " : " +con.getResponseMessage();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ret;
	}

}
