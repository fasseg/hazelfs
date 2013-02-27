package org.hazelfs.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PosixStorageService implements StorageService {

	private static final Logger LOG = LoggerFactory.getLogger(PosixStorageService.class);
	private final File storageDirectory;

	public PosixStorageService(String storageDirectory) {
		this.storageDirectory = new File(storageDirectory);
	}

	public void initStorage() throws IOException {
		if (!storageDirectory.exists()) {
			storageDirectory.mkdir();
		}
		if (!storageDirectory.isDirectory() || !storageDirectory.canWrite() || !storageDirectory.canRead()) {
			throw new IOException("Unable to use " + storageDirectory.getAbsolutePath() + " as a storage directory");
		}
	}

	@Override
	public FileChannel create(String path) throws IOException {
		File f = new File(storageDirectory, path);
		if (f.exists()) {
			throw new IOException("Unabel to overwrite file " + path);
		}
		return FileChannel.open(f.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
	}

	@Override
	public void delete(String path) throws IOException {
		File f = new File(storageDirectory, path);
		if (!f.exists()){
			throw new IOException("File " + f.getAbsolutePath() + " does not exist");
		}
		if (f.isDirectory()){
			throw new IOException("Unable to delete directories");
		}
		LOG.debug("deleting file " + f.getAbsolutePath());
	}

	@Override
	public FileChannel open(String path) throws IOException {
		File f = new File(storageDirectory, path);
		if (!f.exists()){
			throw new IOException("File " + f.getAbsolutePath() + " does not exist");
		}
		if (f.isDirectory()){
			throw new IOException("Unable to open directories");
		}
		return FileChannel.open(f.toPath(), StandardOpenOption.READ);
	}
	
	@Override
	public void format() throws IOException {
		FileUtils.deleteDirectory(storageDirectory);
	}
	
	@Override
	public long getSize(String path) throws IOException {
		File f = new File(storageDirectory,path);
		return f.length();
	}
}
