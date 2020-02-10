package com.distdb.HTTPDataserver.app;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RetCodes {
	
	private static final Gson gson = new GsonBuilder().create();
	
	private String code = "";
	private String message = "";
	private String body = "";
	
	public RetCodes() {
		
	}
	public RetCodes(String retCode) {
		this.code = retCode;
	}
	
	public RetCodes(String retCode, String retMessage) {
		this.code = retCode;
		this.message = retMessage;
	}

	public void setCode(String retCode) {
		this.code = retCode;
	}

	public void setMessage(String retMessage) {
		this.message = retMessage;
	}

	public void setBody(String retBody) {
		this.body = retBody;
	}

	public String getCode() {
		return code;
	}
	public String toJsonString() {
		String ret;
		if (isJSONValid(body)) {
			System.out.println("El body es Json");
			JsonObject jo = new JsonObject();
			JsonParser jp = new JsonParser(); 
			JsonElement je = jp.parse(body);
			
			jo.addProperty("code", gson.toJson(code));
			jo.addProperty("message", gson.toJson(message));
			jo.add("body", je);
			ret = jo.toString();
			System.out.println(jo);
		} else {
			ret = gson.toJson(this);
		}
		return ret;
	}
	
	public static String[] Decode(String jsonString) {
		String[] ret = new String [3];
		RetCodes rc = new Gson().fromJson(jsonString, RetCodes.class);
		ret[0] = rc.code;
		ret[1] = rc.message;
		ret[3] = rc.body;
		return ret;
	}
	
	public static boolean isJSONValid(String jsonInString) {
	      try {
	          gson.fromJson(jsonInString, Object.class);
	          return true;
	      } catch(com.google.gson.JsonSyntaxException ex) { 
	          return false;
	      }
	  }
	
}
