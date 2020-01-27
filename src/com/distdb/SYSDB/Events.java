package com.distdb.SYSDB;

import java.util.Date;

import com.distdb.dbserver.DBTemplate;

public class Events extends DBTemplate {
	String name;
	Date when;
	String type;
	String description;
	
	
	public Events(String name, String type, String description) {
		super ();
		this.name = name;
		this.type = type;
		this.description = description;
		this.when = new Date();
	}

}
