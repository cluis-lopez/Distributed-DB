package com.distdb.TestDB2;

import com.distdb.dbserver.DBTemplate;

public class Cars extends DBTemplate {
	public String model;
	public int horsePower;
	public String brand_id;
	
	public Cars(String model, int horsePower, String brand_id) {
		super();
		this.model = model;
		this.horsePower = horsePower;
		this.brand_id = brand_id;
	}
}
