package org.hazelfs.services;

import static org.junit.Assert.*;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Iterator;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:hazelcast-context.xml" })
public class ManagementServiceTest {

	@Autowired
	@Qualifier("managementService")
	private ManagementService managementService;

	@Test
	public void testNodeLifecycle() throws Exception{
		String id_1 = "node-" + UUID.randomUUID();
		managementService.startNode(id_1);
		Socket sock = new Socket();
		sock.connect(new InetSocketAddress("localhost",ManagementService.NODE_DEFAULT_PORT));
		sock.getOutputStream().write(new byte[]{0x01, 0x41}, 0,2);
		sock.getOutputStream().close();
		managementService.shutdownNode(id_1);
		assertTrue(managementService.getLocalNodes().size() == 0);
	}
}
