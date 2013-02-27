package org.hazelfs.protocol;

public class HazeGet extends HazelRequest {
	private final String path;

	public HazeGet(String path) {
		super(REQUEST_GET);
		this.path = path;
	}

	public String getPath() {
		return path;
	}
}
