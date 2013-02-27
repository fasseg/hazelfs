package org.hazelfs.services;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;

public interface StorageService {
	public FileChannel open(String path) throws IOException;

	public FileChannel create(String path) throws IOException;

	public void delete(String path) throws IOException;
	
	public void format() throws IOException;
	
	public long getSize(String path) throws IOException;
	
}