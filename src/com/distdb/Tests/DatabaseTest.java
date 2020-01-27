package com.distdb.Tests;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.distdb.SYSDB.User;
import com.distdb.dbserver.Database;
import com.distdb.dbserver.DistServer.Type;

class DatabaseTest {
	static Logger log = Logger.getLogger("DistServer");
	static Database db;
	static User u1, u2, u3, u4;

	@BeforeAll
	static void startup() {
		db = new Database(log, "SYS", "SYS.json", "com.distdb.SYSDB", Type.MASTER);
		u1 = new User("clopez", "cluis.lopez@gmail.com", "1234");
		u2 = new User("mariano", "mrajoy@hotmail.com", "1234");
		u3 = new User("juanito", "juanito@hotmail.com", "1234");
	}

	@Test
	void testInsert() {
		//Inserts de objetos tipo User
		String[] test1 = db.insert("User", u1);
		Assertions.assertEquals( "OK", test1[0]);
		String[] test2 = db.insert("User", u2);
		Assertions.assertEquals( "OK", test2[0]);
		String[] test3 = db.insert("User", u3);
		Assertions.assertEquals( "OK", test2[0]);
		Map<String, String> map = db.getInfo();
		Assertions.assertEquals( 3, Integer.parseInt(map.get("User")));
		//Inserts de objetos tipo event haciendo override del id
		
		
	}
	
	@Test
	void testRemove() {
		String test1[] = db.remove("User", "1234");
		Assertions.assertEquals("FAIL", test1[0]);
		String test2[] = db.remove("User", u2.id);
		Assertions.assertEquals("OK", test2[0]);
		Map<String, String> map = db.getInfo();
		Assertions.assertEquals(2, Integer.parseInt(map.get("User")));
	}
	
	@Test
	void testSearch() {
		u4 = (User) db.getById("User", u2.id);
		Assertions.assertEquals(u4, null);
		u4 = (User) db.getById("User", u1.id);
		Assertions.assertEquals(u4, u1);
		
		List<Object> l = db.searchByField("User", "nonexistentfield", "hot");
		Assertions.assertEquals(0, l.size());
		l = db.searchByField("User", "lastLogin", "hot");
		Assertions.assertEquals(0, l.size());
		l = db.searchByField("User", "mail", "hot");
		Assertions.assertEquals(1, l.size());
		Assertions.assertEquals("juanito", ((User)l.get(0)).name);
		u4 = new User("lolita", "lolita@gmail.com", "7890");
		db.insert("User", u4);
		Map<String, String> map = db.getInfo();
		Assertions.assertEquals(3, Integer.parseInt(map.get("User")));
		l = db.searchByField("User", "mail", "gm");
		Assertions.assertEquals(2, l.size());
		Assertions.assertEquals(true, l.contains(u1));
		Assertions.assertEquals(true, l.contains(u4));
	}
	@Test
	void testStore() {
		db.close();
		List<String> result = new ArrayList<>();
		File folder = new File("etc/data/SYS");
		for (final File f : folder.listFiles())
			result.add(f.getName());
		
		Assertions.assertEquals(1, result.size());
		Assertions.assertEquals("_data_User", result.get(0));
		
	}
	
	@Test
	void testReOpen() {
		
	}
}
