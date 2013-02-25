package org.hazelfs.services;

import java.io.Serializable;
import java.net.URI;

public class Node implements Serializable {
	private static final long serialVersionUID = 1L;
	private final String id;
	private final URI uri;
	private final int port;

	protected Node(String id, URI uri) {
		super();
		this.id = id;
		this.uri = uri;
		this.port = uri.getPort();
	}

	public String getId() {
		return id;
	}

	public int getPort() {
		return port;
	}

	public URI getUri() {
		return uri;
	}
}
