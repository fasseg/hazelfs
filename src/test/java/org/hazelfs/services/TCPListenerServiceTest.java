package org.hazelfs.services;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.io.IOUtils;
import org.hazelfs.protocol.HazeFSClient;
import org.hazelfs.protocol.HazeGet;
import org.hazelfs.protocol.HazePut;
import org.hazelfs.protocol.HazeResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:hazefs-context.xml" })
public class TCPListenerServiceTest {
	private static TCPListenerService service;

	@BeforeClass
	public static void setup() throws Exception {
		PosixStorageService storage = new PosixStorageService("target/test-storage");
		storage.format();
		storage.initStorage();
		service = new TCPListenerService(ManagementService.NODE_DEFAULT_PORT, storage);
		Thread t = new Thread(service);
		t.start();
	}

	@Test
	public void putFile() throws Exception {
		HazeFSClient client = new HazeFSClient("localhost", ManagementService.NODE_DEFAULT_PORT);
		client.connect();
		File f = new File("src/test/resources/testfile-junit-1.xml");
		HazePut put = new HazePut("testfile-junit-1.xml",new FileInputStream(f), f.length());
		HazeResponse resp = client.execute(put);
		assertTrue(resp.getCode() == HazeResponse.RETURN_OK);
		resp.close();
		client.disconnect();
	}

	@Test
	public void requestFile() throws Exception {
		HazeFSClient client = new HazeFSClient("localhost", ManagementService.NODE_DEFAULT_PORT);
		client.connect();
		HazeGet get = new HazeGet("testfile-junit-1");
		HazeResponse resp = client.execute(get);
		IOUtils.copy(resp.getData(), System.out);
		resp.close();
		client.disconnect();
	}

	@Test
	public void request2Files() throws Exception {
		HazeFSClient client = new HazeFSClient("localhost", ManagementService.NODE_DEFAULT_PORT);
		client.connect();
		HazeGet get = new HazeGet("testfile-1");
		HazeResponse resp = client.execute(get);
		IOUtils.copy(resp.getData(), System.out);
		resp.close();
		resp = client.execute(get);
		IOUtils.copy(resp.getData(), System.out);
		resp.close();
		client.disconnect();
	}

	@Test
	public void requestFileLeadingSlash() throws Exception {
		HazeFSClient client = new HazeFSClient("localhost", ManagementService.NODE_DEFAULT_PORT);
		client.connect();
		HazeGet get = new HazeGet("/testfile-junit-1");
		HazeResponse resp = client.execute(get);
		client.disconnect();
	}

	@AfterClass
	public static void teardown() throws Exception {
		service.shutdown();
	}

}
