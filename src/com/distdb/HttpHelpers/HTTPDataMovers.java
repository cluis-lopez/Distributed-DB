package com.distdb.HttpHelpers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HTTPDataMovers {

	public static String postData(Logger log, URL url, String database, String service, String postData) {
		String ret = "";
		URL uri = null;
		try {
			
			if (database.equals(""))
				uri = new URL(url + "/" + service);
			else
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

			//Post data send ... waiting for reply
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
		}catch (SocketTimeoutException e) {
			log.log(Level.INFO, "HTTP POST" + uri + " Timeout");
		} catch (ConnectException e) {
			log.log(Level.INFO, "Connection "  + uri + " Refused");
		} catch (IOException e) {
			log.log(Level.WARNING, "cannot send post data to " + uri);
			log.log(Level.WARNING, e.getMessage());
			log.log(Level.WARNING, Arrays.toString(e.getStackTrace()));
			System.err.println(e.getMessage());
			e.printStackTrace();
		}

		return ret;
	}
}
