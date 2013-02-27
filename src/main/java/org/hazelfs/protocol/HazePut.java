package org.hazelfs.protocol;

import java.io.InputStream;

public class HazePut extends HazelRequest {
	private final String path;
	private final InputStream data;
	private final long size;

	public HazePut(String path, InputStream data, long size) {
		super(HazelRequest.REQUEST_PUT);
		this.path = path;
		this.data = data;
		this.size = size;
	}

	public String getPath() {
		return path;
	}

	public InputStream getData() {
		return data;
	}

	public long getSize() {
		return size;
	}

}
