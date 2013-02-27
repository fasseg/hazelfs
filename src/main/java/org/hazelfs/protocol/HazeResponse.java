package org.hazelfs.protocol;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import com.hazelcast.nio.InOutSelector;

public class HazeResponse {
	public static final byte RETURN_ERROR=0xf;
	public static final byte RETURN_OK = 0x0;
	private final int code;
	private final InputStream data;

	protected HazeResponse(byte code, InputStream data) {
		super();
		this.code = code;
		this.data = data;
	}

	public int getCode() {
		return code;
	}

	public InputStream getData() {
		return data;
	}
	
	public void close(){
		IOUtils.closeQuietly(data);
	}

}
