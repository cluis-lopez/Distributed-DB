package com.distdb.dbserver;

import java.util.UUID;

public class DBTemplate {

		public String id;
		private boolean onDisk;
		private boolean isValid;
		
		public DBTemplate() {
			this.id = UUID.randomUUID().toString();
			this.isValid = true;
			this.onDisk = false;
		}
		
		
		public boolean isSaved() {
			return onDisk;
		}

		public void setSaved(boolean saved) {
			this.onDisk = saved;
		}

		public boolean isValid() {
			return isValid;
		}

		public void setValid(boolean valid) {
			this.isValid = valid;
		}
}
