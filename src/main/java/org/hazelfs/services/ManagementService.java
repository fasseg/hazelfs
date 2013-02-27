package org.hazelfs.services;

import java.io.IOException;
import java.util.Map;

public interface ManagementService {
	public static final String NODE_MAP_NAME = "node-map";
	public static final int NODE_DEFAULT_PORT = 3827;

	public void startNode(String id) throws IOException;

	public void shutdownAllNodes();

	public void shutdownNode(String id);

	public Map<String, Node> getLocalNodes();

	public StorageService getStorageService();
}
