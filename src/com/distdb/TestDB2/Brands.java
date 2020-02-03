package com.distdb.TestDB2;

import com.distdb.dbserver.DBTemplate;

public class Brands extends DBTemplate {

	String name;
	String country;
	String market;
	
	public Brands(String name, String country, String market) {
		super();
		this.name = name;
		this.country = country;
		this.market = market;
	}
}
