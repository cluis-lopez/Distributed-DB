package com.distdb.HttpHelpers;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class HelperJson {

	public static String returnCodes(String code, String message, String payload) {
		JsonArray ja = new JsonArray();
		ja.add(code);
		ja.add(message);
		if (payload.length() > 0) {
			JsonElement je = new JsonParser().parse(payload);
			if (je.isJsonObject())
				ja.add(je.getAsJsonObject().toString());
			if (je.isJsonArray())
				ja.add(je.getAsJsonArray().toString());
		} else
			ja.add("");
		return ja.toString();
	}

	public static String[] decodeCodes(String response) {
		String[] resp = new String[3];
		JsonElement je = new JsonParser().parse(response);
		if (je.isJsonArray()) {
			JsonArray ja = je.getAsJsonArray();
			resp[0] = ja.get(0).getAsString();
			resp[1] = ja.get(1).getAsString();
			resp[2] = ja.get(2).getAsString();
		} else {
			resp[0] = "FAIL";
			resp[1] = "Got answer with bad format";
		}
		return resp;
	}
}
