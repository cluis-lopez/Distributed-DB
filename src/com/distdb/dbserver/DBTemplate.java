package com.distdb.dbserver;

import java.util.UUID;

public class DBTemplate {

		public String id;
		
		public DBTemplate() {
			this.id = UUID.randomUUID().toString();
		}
}
