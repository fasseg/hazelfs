package org.hazelfs.services;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hazelfs.networking.HazeFSClient;
import org.hazelfs.networking.HazelFSListener;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:hazefs-context.xml" })
public class HazeFSListenerTest {
	private static HazelFSListener service;
	private static ExecutorService executor;

	@BeforeClass
	public static void setup() throws Exception {
		PosixStorageService storage = new PosixStorageService("target/test-storage");
		storage.format();
		storage.initStorage();
		service = new HazelFSListener(ManagementService.NODE_DEFAULT_PORT, storage);
		executor = Executors.newSingleThreadExecutor();
		executor.submit(service);
	}

	@Test
	public void putAndRequestFile() throws Exception {
		String filename="testfile-junit-1.xml";
		// first put the file
		HazeFSClient client = new HazeFSClient(ManagementService.NODE_DEFAULT_PORT,"localhost");
		client.connect();
		File f = new File ("src/test/resources/" + filename);
		client.create(filename,this.getClass().getClassLoader().getResourceAsStream("testfile-junit-1.xml"),f.length());
		client.open(filename);
//		File f = new File("src/test/resources/testfile-junit-1.xml");
//		HazePut put = new HazePut(filename,new FileInputStream(f), f.length());
//		HazeResponse resp = client.execute(put);
//		assertTrue(resp.getCode() == HazeResponse.RETURN_OK);
//		resp.close();
//		
//		// now request the files
//		HazeGet get = new HazeGet(filename);
//		resp = client.execute(get);
//		assertTrue(resp.getCode() == HazeResponse.RETURN_OK);
//		assertTrue(resp.getStreamSize() == f.length());
		client.disconnect();
	}


	@AfterClass
	public static void teardown() throws Exception {
		service.shutdown();
		Thread.sleep(200);
		executor.shutdownNow();
	}

}
