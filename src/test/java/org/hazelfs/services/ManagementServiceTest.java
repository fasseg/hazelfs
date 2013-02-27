package org.hazelfs.services;

import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.hazelfs.protocol.HazeFSClient;
import org.hazelfs.protocol.HazeGet;
import org.hazelfs.protocol.HazePut;
import org.hazelfs.protocol.HazeResponse;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:hazefs-context.xml" })
public class ManagementServiceTest {

	private static final String nodeID = "id-1";

	@Autowired
	@Qualifier("managementService")
	private ManagementService managementService;
	private static ManagementService INSTANCE;
	boolean init = false;

	@Before
	public void setup() throws Exception {
		if (!init) {
			managementService.startNode(nodeID);
			INSTANCE = managementService;
			init = true;
		}
	}

	@AfterClass
	public static void teardown() throws Exception {
		INSTANCE.shutdownNode(nodeID);
		INSTANCE.getStorageService().format();
	}
}
