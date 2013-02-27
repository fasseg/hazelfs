package org.hazelfs.protocol;

public abstract class HazelRequest {
	public static final String SCHEME = "hazefs";
	public static final int REQUEST_GET = 1;
	public static final int REQUEST_PUT = 2;
	
	private final int type;

	protected HazelRequest(int type) {
		this.type = type;
	}
	
	public final int getType() {
		return type;
	}
}
