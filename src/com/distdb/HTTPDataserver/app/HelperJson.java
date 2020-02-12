package com.distdb.HTTPDataserver.app;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class HelperJson {

	public static String returnCodes(String code, String message, String payload) {
		JsonArray ja = new JsonArray();
		ja.add(code);
		ja.add(message);
		if (payload.length()>0) {
			JsonElement je = new JsonParser().parse(payload);
			if (je.isJsonObject())
				ja.add(je.getAsJsonObject());
			if (je.isJsonArray())
				ja.add(je.getAsJsonArray());
		}
		else
			ja.add("");
		return ja.toString();
	}
}
