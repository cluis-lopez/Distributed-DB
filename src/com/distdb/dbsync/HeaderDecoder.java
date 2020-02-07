package com.distdb.dbsync;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public class HeaderDecoder {

	public String command;
	public String resource;
	public String protocol;
	public Map<String, String> params;

	public HeaderDecoder(String line) throws UnsupportedEncodingException {
		command = "";
		resource = "";
		protocol = "";
		params = null;
		String temp[] = line.split(" ");
		if (temp.length < 3)
			return;
		command = temp[0];
		protocol = temp[2];
		temp = temp[1].split("\\?");
		resource = temp[0];
		params = new HashMap<>();
		if (temp.length == 2) { // Hay parametros
			for (String s : temp[1].split("&")) {
				int idx = s.indexOf("=");
				params.put(URLDecoder.decode(s.substring(0, idx), "UTF-8"),
						URLDecoder.decode(s.substring(idx + 1), "UTF-8"));
			}
		}
	}

	public String toString() {
		String ret = "Comando: " + command + "\n";
		ret += "Protocolo: " + protocol + "\n";
		ret += "Recurso: " + resource + "\n";
		ret += "Parametros: ";
		if (params == null || params.size() == 0) {
			ret += "No hay parametros";
		} else {
			for (String s : params.keySet())
				ret += "\n" + s + " : " + params.get(s);
		}
		return ret;
	}

}
