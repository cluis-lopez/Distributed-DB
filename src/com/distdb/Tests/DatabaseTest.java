package com.distdb.Tests;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.distdb.TestDB1.Event;
import com.distdb.TestDB1.User;
import com.distdb.dbserver.Database;
import com.distdb.dbserver.MasterDatabase;
import com.distdb.dbsync.MasterSyncer;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

@TestMethodOrder(OrderAnnotation.class)
public class DatabaseTest {
	static Logger log = Logger.getLogger("DistServer");
	static MasterSyncer dsync;
	static Database db;
	static User u1, u2, u3, u4;
	static Event e1, e2, e3, e4;

	@BeforeAll
	static void startup() {

		// Remove all data files from TestDB database ... Warning !!!

		Path folder = Paths.get("etc/data/TestDB1/");
		Stream<Path> list;
		try {
			list = Files.list(folder);

			for (Path f : list.toArray(Path[]::new)) {
				Files.delete(f);
				System.err.println("Borrado el fichero: " + f.toString() + " ");
			}
			list.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		dsync = new MasterSyncer(log, null, 30);
		db = new MasterDatabase(log, "TestDB1", "TestDB1.json", "com.distdb.TestDB1", dsync);
		u1 = new User("clopez", "clopez@gmail.com", "1234");
		u2 = new User("mariano", "mrajoy@hotmail.com", "1234");
		u3 = new User("juanito", "juanito@hotmail.com", "1234");
		e1 = new Event("Evento1", "Malisimo", "cosa chunga ca'pasao");
		e2 = new Event("Evento2", "Preocupante", "... al loro ...");
	}

	@Test
	@Order(1)
	void testInsert() {
		String[] s = db.open();
		Assertions.assertTrue(s[0].equals("OK"));
		// Inserts de objetos tipo User
		String[] test1 = db.insert("User", u1);
		Assertions.assertEquals("OK", test1[0]);
		String[] test2 = db.insert("User", u2);
		Assertions.assertEquals("OK", test2[0]);
		String[] test3 = db.insert("User", u3);
		Assertions.assertEquals("OK", test3[0]);
		JsonObject jo = new JsonParser().parse(db.getInfo()).getAsJsonObject();
		Assertions.assertEquals(3, jo.getAsJsonPrimitive("User").getAsInt());
		// Inserts de objetos tipo event haciendo override del id
		String[] test4 = db.insert("Event", e1);
		Assertions.assertEquals("OK", test4[0]);
		String[] test5 = db.insert("Event", e2);
		Assertions.assertEquals("OK", test5[0]);
		// Insercion de objeto no existente
		String[] test6 = db.insert("ObjetoInexistente", e2);
		Assertions.assertEquals("Invalid Object", test6[1]);
		// Insercion de objeto existente pero con la clase confundida
		String[] test7 = db.insert("User", e2);
		Assertions.assertEquals("Invalid Object", test7[1]);
		jo = new JsonParser().parse(db.getInfo()).getAsJsonObject();
		Assertions.assertEquals(2, jo.getAsJsonPrimitive("Event").getAsInt());

	}

	@Test
	@Order(2)
	void testRemove() {
		String test1[] = db.remove("User", "1234");
		Assertions.assertEquals("FAIL", test1[0]);
		String test2[] = db.remove("User", u2.id);
		Assertions.assertEquals("OK", test2[0]);
		JsonObject jo = new JsonParser().parse(db.getInfo()).getAsJsonObject();
		Assertions.assertEquals(2, jo.getAsJsonPrimitive("User").getAsInt());
	}

	@Test
	@Order(3)
	void testSearch() {
		u4 = (User) new Gson().fromJson(db.getById("User", u2.id)[2], User.class);
		Assertions.assertEquals(u4, null);
		u4 = (User) new Gson().fromJson(db.getById("User", u1.id)[2], User.class);
		;
		Assertions.assertEquals(u4.id, u1.id);
		e3 = (Event) new Gson().fromJson(db.getById("Event", "Evento2")[2], Event.class);
		Assertions.assertEquals(e2.id, e3.id);

		List<User> l = new Gson().fromJson(db.searchByField("User", "nonexistentfield", "hot")[2], List.class);
		Assertions.assertEquals(0, l.size());
		l = new Gson().fromJson(db.searchByField("User", "lastLogin", "hot")[2], List.class);
		Assertions.assertEquals(0, l.size());

		java.lang.reflect.Type dt = TypeToken.getParameterized(List.class, User.class).getType();
		l = new Gson().fromJson(db.searchByField("User", "mail", "hot")[2], dt);
		Assertions.assertEquals(1, l.size());

		Assertions.assertEquals("juanito", ((User) l.get(0)).name);
		u4 = new User("lolita", "lolita@gmail.com", "7890");
		db.insert("User", u4);
		JsonObject jo = new JsonParser().parse(db.getInfo()).getAsJsonObject();
		Assertions.assertEquals(3, jo.getAsJsonPrimitive("User").getAsInt());
		l = new Gson().fromJson(db.searchByField("User", "mail", "gm")[2], dt);
		Assertions.assertEquals(2, l.size());
		Assertions.assertTrue(l.get(0).name.equals("clopez") || l.get(1).name.equals("clopez"));
		Assertions.assertTrue(l.get(0).name.equals("lolita") || l.get(1).name.equals("lolita"));
	}

	@Test
	@Order(4)
	void testStore() {
		db.close();
		List<String> result = new ArrayList<>();
		File folder = new File("etc/data/TestdB1");
		for (final File f : folder.listFiles())
			result.add(f.getName());
		Assertions.assertEquals(4, result.size());
		Assertions.assertEquals(true, result.contains("_data_User"));
		Assertions.assertEquals(true, result.contains("_data_Event"));

	}

	@Test
	void testReOpen() {
		db = new MasterDatabase(log, "TestDB1", "TestDB1.json", "com.distdb.TestDB1", dsync);
		db.open();
		JsonObject jo = new JsonParser().parse(db.getInfo()).getAsJsonObject();
		Assertions.assertEquals(3, jo.getAsJsonPrimitive("User").getAsInt());
		Assertions.assertEquals(2, jo.getAsJsonPrimitive("Event").getAsInt());

		Object o = db.getById("User", u1.id);
		System.err.println("Tipo de objeto " + o.getClass().getTypeName());
		User u5 = new Gson().fromJson(db.getById("User", u3.id)[2], User.class);
		Assertions.assertEquals(true, u5.id.equals(u3.id));
		// Assertions.assertEquals(u4, u1);
		e4 = new Gson().fromJson(db.getById("Event", "Evento2")[2], Event.class);
		Assertions.assertEquals("Preocupante", e4.type);

		// Insertamos nuevos valores en la coleccion de usuarios
		User u10 = new User("nuevoclopez", "nuevoclopez@gmail.com", "1234");
		User u11 = new User("nuevalolita", "nuevalolita@hotamil.com", "1234");
		Event e10 = new Event("Evento10", "Jodidillo", "... al loro ...");
		db.insert("User", u10);
		db.insert("User", u11);
		db.insert("Event", e10);
		db.close();

		// Reabrimos la BBDD Ahora deber�a de haber 5 usuarios y 3 eventos

		db = new MasterDatabase(log, "TestDB1", "TestDB1.json", "com.distdb.TestDB1", dsync);
		db.open();
		jo = new JsonParser().parse(db.getInfo()).getAsJsonObject();
		Assertions.assertEquals(3, jo.getAsJsonPrimitive("Event").getAsInt());
		Assertions.assertEquals(5, jo.getAsJsonPrimitive("User").getAsInt());
		db.close();
	}
}
