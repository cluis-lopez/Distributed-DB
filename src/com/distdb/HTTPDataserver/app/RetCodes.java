package com.distdb.HTTPDataserver.app;

import com.google.gson.Gson;

public class RetCodes {
	
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
		return new Gson().toJson(this);
	}
}
