package com.distdb.Tests;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.distdb.TestDB1.Event;
import com.distdb.TestDB1.User;
import com.distdb.TestDB2.Brands;
import com.distdb.TestDB2.Cars;
import com.distdb.dbserver.Database;
import com.distdb.dbserver.DistServer.DBType;
import com.distdb.dbsync.DiskSyncer;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

@TestMethodOrder(OrderAnnotation.class)
class DiskSyncerTest {

	static Logger log = Logger.getLogger("DistServer");
	static final int waitTime = 10 * 1000;
	static Database db1;
	static Database db2;
	static DiskSyncer dSyncer;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		dSyncer = new DiskSyncer(log, waitTime);

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

		db1 = new Database(log, "TestDB1", "TestDB1.json", "com.distdb.TestDB1", dSyncer, DBType.MASTER);
		db2 = new Database(log, "TestDB2", "TestDB2.json", "com.distdb.TestDB2", dSyncer, DBType.MASTER);
	}

	@Test
	void testAddDatabase() {
		dSyncer.addDatabase("TestDB1", db1.dbobjs, db1.dataPath);
		dSyncer.addDatabase("TestDB2", db2.dbobjs, db2.dataPath);
		Assertions.assertEquals(2, dSyncer.dataPaths.size());
		Assertions.assertEquals(2, dSyncer.dbQueue.size());
		Assertions.assertEquals(0, dSyncer.dbQueue.get("TestDB1").size());
	}

	@SuppressWarnings("deprecation")
	@Test
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
		Assertions.assertEquals(3, dSyncer.dbQueue.get("TestDB1").size());
		Assertions.assertEquals(4, dSyncer.dbQueue.get("TestDB2").size());

		// Wait for the Syncer to update disk logs

		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// Now add a new element and remove first elements for each dataset

		db1.remove("User", u1.id);
		db1.remove("Event", e1.id);
		db2.remove("Brands", b1.id);
		db2.remove("Cars", c1.id);
		db1.insert("Event",  new Event("Evento2", "Peligroso", "no hay mensaje"));
		// Wait for the Syncer to finish

		try {
			Thread.sleep(20000);
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
		//db1 = null;
		//db2 = null;
		dSyncer.kill();
		dSyncer = null;
		//Reopen databases
		
		System.out.println("Reopen Databases");
		dSyncer = new DiskSyncer(log, waitTime);
		db1 = new Database(log, "TestDB1", "TestDB1.json", "com.distdb.TestDB1", dSyncer, DBType.MASTER);
		db2 = new Database(log, "TestDB2", "TestDB2.json", "com.distdb.TestDB2", dSyncer, DBType.MASTER);
		dSyncer.addDatabase("TestDB1", db1.dbobjs, db1.dataPath);
		dSyncer.addDatabase("TestDB2", db2.dbobjs, db2.dataPath);
		
		Assertions.assertEquals(b2.id, ((Brands) db2.getById("Brands", b2.id)).id);
		Assertions.assertEquals("Ferrari", ((Brands) db2.searchByField("Brands", "name", "ari").get(0)).name);
		db1.close();
		db2.close();
		dSyncer.kill();
		
	}

}
