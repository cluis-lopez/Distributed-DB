package com.distdb.TestDB;

import java.util.Date;

import com.distdb.dbserver.DBTemplate;

public class Event extends DBTemplate {
	String name;
	Date when;
	String type;
	String description;
	
	
	public Event(String name, String type, String description) {
		id = name;
		this.type = type;
		this.description = description;
		this.when = new Date();
	}

}
