package com.distdb.Tests;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.distdb.HttpHelpers.HelperJson;
import com.distdb.TestDB1.Event;
import com.distdb.TestDB1.User;
import com.distdb.TestDB2.Brands;
import com.distdb.TestDB2.Cars;
import com.distdb.dbserver.MasterDatabase;
import com.distdb.dbserver.Node;
import com.distdb.dbserver.Cluster;
import com.distdb.dbserver.DistServer.DBType;
import com.distdb.dbsync.MasterSyncer;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

@TestMethodOrder(OrderAnnotation.class)
class DiskSyncerTest {

	static Logger log = Logger.getLogger("DistServer");
	static final int waitTime = 5 * 1000;
	static MasterDatabase db1;
	static MasterDatabase db2;
	static MasterSyncer dSyncer;
	static Cluster cluster;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		List<Map<String, String>> nodes = new ArrayList<>();
		Map<String, String> entryMaster = new HashMap<>();
		entryMaster.put("name", "master1");
		entryMaster.put("nodeType", "Master");
		entryMaster.put("url", "http://localhost:8081");
		nodes.add(entryMaster);
		Map<String, String> entryReplica = new HashMap<>();
		entryReplica.put("name", "replica1");
		entryReplica.put("nodeType", "Replica");
		entryReplica.put("url", "http://localhost:8083");
		nodes.add(entryReplica);
		cluster = new Cluster(log, "master1", nodes, DBType.MASTER);
		cluster.setMaster();
		cluster.setReplicas();
		dSyncer = new MasterSyncer(log, cluster, waitTime);

		// Borramos las BBDD de Test

		Path[] folder = new Path[2];
		folder[0] = Paths.get("etc/data/TestDB1/");
		folder[1] = Paths.get("etc/data/TestDB2/");
		for (int i = 0; i < 2; i++) {
			Stream<Path> list;
			try {
				list = Files.list(folder[i]);

				for (Path f : list.toArray(Path[]::new)) {
					Files.delete(f);
					System.err.println("Borrado el fichero: " + f.toString() + " ");
				}
				list.close();
			} catch (IOException e) {
				System.err.println("No se pueden borrar los ficheros de las BBDD");
			}
		}

		db1 = new MasterDatabase(log, "TestDB1", "TestDB1.json", "com.distdb.TestDB1", dSyncer);
		String[] s = db1.open();
		Assertions.assertTrue(s[0].equals("OK"));
		db2 = new MasterDatabase(log, "TestDB2", "TestDB2.json", "com.distdb.TestDB2", dSyncer);
		s = db2.open();
		Assertions.assertTrue(s[0].equals("OK"));
	}

	@Test
	@Order(1)
	void testAddDatabase() {
		dSyncer.addDatabase("TestDB1", db1.dataPath);
		dSyncer.addDatabase("TestDB2", db2.dataPath);
		Assertions.assertEquals(2, dSyncer.dataPaths.size());
		Assertions.assertEquals(0, dSyncer.logOps.size());
	}

	@SuppressWarnings("deprecation")
	@Test
	@Order(2)
	void testDiskSyncer() {
		Thread t = new Thread(dSyncer);
		t.start();

		// Start adding objects to databases
		User u1 = new User("clopez", "clopez@gmail.com", "1234");
		User u2 = new User("juanito", "juanito@hotmail.com", "4321");
		Event e1 = new Event("Evento1", "Malo", "... que putada...");
		db1.insert("User", u1);
		db1.insert("User", u2);
		db1.insert("Event", e1);
		Brands b1 = new Brands("Opel", "Germany", "simple cars");
		Brands b2 = new Brands("Ferrari", "Italy", "sport cars");
		Cars c1 = new Cars("Corsa", 110, b1.id);
		Cars c2 = new Cars("Testarrosa", 1200, b2.id);
		db2.insert("Brands", b1);
		db2.insert("Brands", b2);
		db2.insert("Cars", c1);
		db2.insert("Cars", c2);
		Assertions.assertEquals(7,  dSyncer.logOps.size());
		Assertions.assertEquals(3, dSyncer.dbQueue.get("TestDB1").size());
		Assertions.assertEquals(4, dSyncer.dbQueue.get("TestDB2").size());

		// Wait for the Syncer to update disk logs

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// Now add a new element and remove first elements for each dataset

		db1.remove("User", u1.id);
		db1.remove("Event", e1.id);
		db2.remove("Brands", b1.id);
		db2.remove("Cars", c1.id);
		db1.insert("Event", new Event("Evento2", "Peligroso", "no hay mensaje"));
		// Wait for the Syncer to finish

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Map<String, String> mapa = dSyncer.getInfoFromLogFiles();
		Assertions.assertEquals(4, Integer.parseInt(mapa.get("Pending inserts for TestDB1")));
		Assertions.assertEquals(4, Integer.parseInt(mapa.get("Pending inserts for TestDB2")));
		Assertions.assertEquals(2, Integer.parseInt(mapa.get("Pending removes for TestDB1")));
		Assertions.assertEquals(2, Integer.parseInt(mapa.get("Pending removes for TestDB2")));

		db1.close();
		db2.close();
		// Equivalente a abortar el programa
		// db1 = null;
		// db2 = null;
		dSyncer.kill();
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// Esta vido el syncer ???
		Assertions.assertFalse(t.isAlive());
		dSyncer = null;
		db1 = null;
		db2 = null;
		// Reopen databases

		System.out.println("Reopen Databases");
		MasterSyncer dSyncer2 = new MasterSyncer(log, cluster, waitTime, waitTime);
		MasterDatabase db12 = new MasterDatabase(log, "TestDB1", "TestDB1.json", "com.distdb.TestDB1", dSyncer2);
		String[] s = db12.open();
		Assertions.assertTrue(s[0].equals("OK"));
		MasterDatabase db22 = new MasterDatabase(log, "TestDB2", "TestDB2.json", "com.distdb.TestDB2", dSyncer2);
		s = db22.open();
		Assertions.assertTrue(s[0].equals("OK"));
		dSyncer2.addDatabase("TestDB1", db12.dbobjs, db12.dataPath);
		dSyncer2.addDatabase("TestDB2", db22.dbobjs, db22.dataPath);

		Thread t2 = new Thread(dSyncer2);
		t2.start();

		User temp = new Gson().fromJson(db22.getById("Brands", b2.id)[2], User.class);
		Assertions.assertEquals(b2.id, temp.id);

		java.lang.reflect.Type dt = TypeToken.getParameterized(List.class, Brands.class).getType();
		List<Brands> l = new Gson().fromJson(db22.searchByField("Brands", "name", "ari")[2], dt);
		Assertions.assertTrue(l.get(0).name.equals("Ferrari"));

		db12.close();
		db22.close();
		dSyncer2.kill();
		try {
			Thread.sleep(10000); // To ensure the current syncer dies
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// Esta vido el syncer ???
		Assertions.assertFalse(t2.isAlive());
		dSyncer = null;

	}

	@SuppressWarnings("deprecation")
	@Test
	@Order(3)
	void testDiskSyncerWithoutDelay() {
		System.out.println("Test con logs directos a disco");
		// Arrancamos el syncer con delaylog = 0 (direct-to-disk)
		MasterSyncer dSyncer3 = new MasterSyncer(log, cluster, 0, waitTime);
		MasterDatabase db13 = new MasterDatabase(log, "TestDB1", "TestDB1.json", "com.distdb.TestDB1", dSyncer3);
		dSyncer3.addDatabase("TestDB1", db13.dbobjs, db13.dataPath);

		Thread t3 = new Thread(dSyncer3);
		t3.start();

		String[] s = db13.open();
		Assertions.assertTrue(s[0].equals("OK"));

		User u1 = new User("clopez", "clopez@gmail.com", "1234");
		User u2 = new User("juanito", "juanito@hotmail.com", "4321");
		Event e1 = new Event("Evento1", "Malo", "... que putada...");
		db13.insert("User", u1);
		db13.insert("User", u2);
		db13.insert("Event", e1);

		// las tres inserciones directamente al fichero de log

		try {
			FileReader fr = new FileReader("etc/data/TestDB1/TestDB1_logging");
			JsonElement je = new JsonParser().parse(fr);
			fr.close();
			Assertions.assertEquals(3, je.getAsJsonArray().size());
		} catch (JsonIOException | JsonSyntaxException | IOException e) {
			e.printStackTrace();
		}

		db13.close();
		dSyncer3.kill();
		dSyncer3 = null;
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		Assertions.assertFalse(t3.isAlive());

		MasterSyncer dSyncer4 = new MasterSyncer(log, cluster, 0, waitTime);
		MasterDatabase db14 = new MasterDatabase(log, "TestDB1", "TestDB1.json", "com.distdb.TestDB1", dSyncer4);
		dSyncer4.addDatabase("TestDB1", db14.dbobjs, db14.dataPath);

		s = db14.open();
		Assertions.assertTrue(s[0].equals("OK"));

		s = db14.searchByField("User", "name", "clopez");
		List<User> l = new Gson().fromJson(s[2], List.class);
		Assertions.assertEquals(1, l.size());
		User u4 = (User) l.get(0);
		Assertions.assertEquals("clopez", u4.name);

	}
}
