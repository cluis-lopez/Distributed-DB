package com.distdb.dbserver;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distdb.dbsync.DiskSyncer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ReplicaDatabase extends Database {

	public String dbname;
	public String defPath;
	private Logger log;
	private String propsFile;
	private Properties props;

	public Map<String, DBObject> dbobjs;
	
	public ReplicaDatabase(Logger log, String dbname, String config, String defPath, URL masterSyncServer) {
		super(log, dbname, config, defPath);
	}
	
	

	@Override
	public String[] open() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] close() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] sync() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] insert(String objectName, Object o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] insert(String objectName, Object object, boolean logging) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] remove(String objectName, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] remove(String objectName, String id, boolean logging) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getById(String objectName, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] searchByField(String objectName, String fieldName, String value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getInfo() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private void updateProps() {
		try {
			FileWriter fw = new FileWriter(propsFile);
			Gson json = new GsonBuilder().setPrettyPrinting().create();
			fw.write(json.toJson(props, Properties.class));
			fw.close();
		} catch (IOException e) {
			log.log(Level.SEVERE, "Cannot prtoperly update the Properties file for " + dbname);
			log.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
		}
	}
	
}