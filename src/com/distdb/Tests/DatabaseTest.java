package com.distdb.Tests;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.distdb.TestDB.Event;
import com.distdb.TestDB.User;
import com.distdb.dbserver.Database;
import com.distdb.dbserver.DistServer.Type;
import com.distdb.dbsync.DiskSyncer;

@TestMethodOrder(OrderAnnotation.class)
public class DatabaseTest {
	static Logger log = Logger.getLogger("DistServer");
	static DiskSyncer dsync;
	static Database db;
	static User u1, u2, u3, u4;
	static Event e1, e2, e3;

	@BeforeAll
	static void startup() {
		dsync = new DiskSyncer(0);
		db = new Database(log, "TestDB", "TestDB.json", "com.distdb.TestDB", dsync, Type.MASTER);
		u1 = new User("clopez", "clopez@gmail.com", "1234");
		u2 = new User("mariano", "mrajoy@hotmail.com", "1234");
		u3 = new User("juanito", "juanito@hotmail.com", "1234");
		e1 = new Event("Evento1", "Malisimo", "cosa chunga ca'pasao");
		e2 = new Event("Evento2", "Preocupante", "... al loro ...");

		// Remove all data files from TestDB database ... Warning !!!

		File folder = new File("etc/data/TestDB/");
		for (final File f : folder.listFiles()) {
			if (f.isFile()) {
				System.out.println("Fichero: " + f.getName());
				boolean b = f.delete();
				System.err.println("Borrado el fichero: " + f.getName()+ " "+ b);
			}
		}
	}

	@Test
	@Order(1)
	void testInsert() {
		// Inserts de objetos tipo User
		String[] test1 = db.insert("User", u1);
		Assertions.assertEquals("OK", test1[0]);
		String[] test2 = db.insert("User", u2);
		Assertions.assertEquals("OK", test2[0]);
		String[] test3 = db.insert("User", u3);
		Assertions.assertEquals("OK", test3[0]);
		Map<String, String> map = db.getInfo();
		Assertions.assertEquals(3, Integer.parseInt(map.get("User")));
		// Inserts de objetos tipo event haciendo override del id
		String[] test4 = db.insert("Event", e1);
		Assertions.assertEquals("OK", test4[0]);
		String[] test5 = db.insert("Event", e2);
		Assertions.assertEquals("OK", test5[0]);
		map = db.getInfo();
		Assertions.assertEquals(2, Integer.parseInt(map.get("Event")));

	}

	@Test
	@Order(2)
	void testRemove() {
		String test1[] = db.remove("User", "1234");
		Assertions.assertEquals("FAIL", test1[0]);
		String test2[] = db.remove("User", u2.id);
		Assertions.assertEquals("OK", test2[0]);
		Map<String, String> map = db.getInfo();
		Assertions.assertEquals(2, Integer.parseInt(map.get("User")));
	}

	@Test
	@Order(3)
	void testSearch() {
		u4 = (User) db.getById("User", u2.id);
		Assertions.assertEquals(u4, null);
		u4 = (User) db.getById("User", u1.id);
		Assertions.assertEquals(u4, u1);
		e3 = (Event) db.getById("Event", "Evento2");
		Assertions.assertEquals(e2, e3);

		List<Object> l = db.searchByField("User", "nonexistentfield", "hot");
		Assertions.assertEquals(0, l.size());
		l = db.searchByField("User", "lastLogin", "hot");
		Assertions.assertEquals(0, l.size());
		l = db.searchByField("User", "mail", "hot");
		Assertions.assertEquals(1, l.size());
		Assertions.assertEquals("juanito", ((User) l.get(0)).name);
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
	@Order(4)
	void testStore() {
		db.close();
		List<String> result = new ArrayList<>();
		File folder = new File("etc/data/TestdB");
		for (final File f : folder.listFiles())
			result.add(f.getName());
		System.out.println(result.get(0));
		Assertions.assertEquals(2, result.size());
		Assertions.assertEquals(true, result.contains("_data_User"));
		Assertions.assertEquals(true, result.contains("_data_Event"));

	}

	@Test
	void testReOpen() {

	}
}
