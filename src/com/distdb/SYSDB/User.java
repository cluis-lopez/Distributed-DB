package com.distdb.SYSDB;

import java.util.Calendar;
import java.util.Date;

import com.distdb.dbserver.DBTemplate;

public class User extends DBTemplate {
	public String name;
	public String mail;
	public String password;
	public Date userSince;
	public Date lastLogin;
	
	public User(String name, String mail, String password) {
		super();
		this.name = name;
		this.mail = mail;
		this.password = password;
		userSince = new Date();
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, 1900);
		cal.set(Calendar.MONTH, Calendar.JANUARY);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		lastLogin = cal.getTime();
				
	}
}
