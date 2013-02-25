package org.hazelfs.services;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

@Service("managementService")
public class DefaultManagementService implements ManagementService {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultManagementService.class);

	@Autowired
	@Qualifier("hazelCastConfig")
	private Config hazelCastConfig;

	private Map<String, HazelcastInstance> localHCInstances = new HashMap<String, HazelcastInstance>();
	private Map<String, Node> localNodes = new HashMap<String, Node>();
	private Map<String, TCPService> localTCPServices = new HashMap<String, TCPService>();

	public void startNode(String id) throws IOException {
		LOG.debug("starting new hazelcast instance");
		hazelCastConfig.setProperty("hazelcast.logging.type", "slf4j");
		HazelcastInstance instance = Hazelcast.newHazelcastInstance(hazelCastConfig);
		Map<String, Node> nodeMap = instance.getMap(ManagementService.NODE_MAP_NAME);
		Node n = createAndRunNode(id);
		localNodes.put(n.getId(), n);
		localHCInstances.put(n.getId(), instance);
		nodeMap.put(id, n);
	}

	private Node createAndRunNode(String id) throws IOException {
		int port = getNextFreePort(ManagementService.NODE_DEFAULT_PORT);
		URI u = URI.create("hazefs://" + Inet4Address.getLocalHost().getHostAddress() + ":"
				+ port);
		LOG.info("starting hazefs node at " + u.toASCIIString());
		Node n = new Node(id, u);
		// start the TCP service for this node
		TCPService tcp = new TCPService(port);
		Thread t = new Thread(tcp,"hazelfs-tcp-service");
		t.start();
		localTCPServices.put(id, tcp);
		return n;
	}

	private int getNextFreePort(int port) {
		boolean free = true;
		while (!isPortFree(port)) {
			port++;
		}
		return port;
	}

	private boolean isPortFree(int port) {
		for (Node n : localNodes.values()) {
			if (n.getPort() == port) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void shutdownNode(String id) {
		HazelcastInstance instance = localHCInstances.get(id);

		// remove the node from the maps
		localNodes.remove(id);
		instance.getMap(ManagementService.NODE_MAP_NAME).remove(id);
		
		//Stop the TCP Service for the Node 
		TCPService service = localTCPServices.get(id);
		service.shutdown();
		
		// remove the HazelCast instances from the map and stop them
		localHCInstances.remove(id);
		instance.getLifecycleService().shutdown();
	}

	@Override
	public void shutdownAllNodes() {
		LOG.debug("shutting down all hazelcast instances");
		for (String id : localNodes.keySet()) {
			this.shutdownNode(id);
		}
	}

	@Override
	public Map<String, Node> getLocalNodes() {
		return localNodes;
	}

}
