package com.distdb.TestDB1;

import java.util.Date;

import com.distdb.dbserver.DBTemplate;

public class Event extends DBTemplate {
	public String name;
	public Date when;
	public String type;
	public String description;
	
	
	public Event(String name, String type, String description) {
		id = name;
		this.type = type;
		this.description = description;
		this.when = new Date();
	}

}
