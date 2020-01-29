package com.distdb.SYSDB;

import java.util.Calendar;
import java.util.Date;

public class User {
		public String Id;
		public String Name;
		public String Mail;
		public String Password;
		public String Salt;
		public String Token;
		public Date UserSince;
		public Date LastLogin;
		public Date TokenValidUpTo;
		public boolean Blocked;
		
		public User(String Id, String Name, String Password) {
			this.Id = Id;
			this.Name = Name;
			Mail = Id;
			String[] pass = encryptPasswd(Password);
			this.Password = pass[0];
			Salt = pass[1];
			Token = "";
			UserSince = new Date();
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.YEAR, 1900);
			cal.set(Calendar.MONTH, Calendar.JANUARY);
			cal.set(Calendar.DAY_OF_MONTH, 1);
			LastLogin = cal.getTime();
			TokenValidUpTo = LastLogin;
			Blocked = false;
		}
		
		public String[] encryptPasswd(String password) {
			String[] pass = new String[2];
			//TBD
			return pass;
		}
}
