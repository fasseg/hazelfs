package org.hazelfs.networking;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazeFSClient {

	private static final Logger LOG = LoggerFactory.getLogger(HazeFSClient.class);
	private final int port;
	private final String host;
	private final Charset utf8 = Charset.forName("UTF-8");
	private final CharsetEncoder encoder = utf8.newEncoder();
	private final CharsetDecoder decoder = utf8.newDecoder();
	private SocketChannel channel;

	public HazeFSClient(int port, String host) {
		super();
		this.port = port;
		this.host = host;
	}

	public void connect() throws IOException {
		channel = SocketChannel.open(new InetSocketAddress(host, port));
	}

	public void disconnect() throws IOException {
		channel.close();
	}

	public InputStream open(String path) throws IOException {
		writeInt(Constants.PROTO_REQUEST_FILE);

		// write the length of the parameter to the server
		CharBuffer chars = CharBuffer.wrap(path.toCharArray());
		ByteBuffer src = encoder.encode(chars);
		writeInt(src.limit());

		// write the path to the server
		while (src.hasRemaining()) {
			int numWritten = channel.write(src);
			LOG.debug("wrote " + numWritten + " bytes to server");
		}

		// expect the long size value from the server
		long size = readLong();
		if (size == 0) {
			throw new IOException("File hazefs://" + host + ":" + port + "/" + path + " does not exist");
		}
		return null;
	}

	private long readLong() throws IOException {
		ByteBuffer sizeBuffer = ByteBuffer.allocate(8);
		while (sizeBuffer.position() < 8) {
			channel.read(sizeBuffer);
		}
		sizeBuffer.flip();
		return sizeBuffer.getLong();
	}

	private int readInt() throws IOException {
		ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
		while (sizeBuffer.position() < 4) {
			channel.read(sizeBuffer);
		}
		sizeBuffer.flip();
		return sizeBuffer.getInt();
	}

	public void create(String path, InputStream data, long size) throws IOException {
		writeInt(Constants.PROTO_CREATE_FILE);

		// write the length of the parameter to the server
		CharBuffer chars = CharBuffer.wrap(path.toCharArray());
		ByteBuffer src = encoder.encode(chars);
		writeInt(src.limit());

		// write the path to the server
		while (src.hasRemaining()) {
			int numWritten = channel.write(src);
			LOG.debug("wrote " + numWritten + " bytes to server");
		}

		// request the answercode
		int answerCode = readInt();
		if (answerCode == Constants.PROTO_CREATE_FILE) {
			LOG.debug("Server is ready for upload of " + path);
		}

		// write the streamsize to the server
		writeLong(size);

		// write the stream to the server
		writeStream(data,size);
	}

	private void writeStream(InputStream data, long size) throws IOException {
		byte[] buf = new byte[512];
		int read = 0;
		int written = 0;
		while (written < size) {
			read = data.read(buf);
			ByteBuffer src = ByteBuffer.wrap(buf,0,read);
			while (src.hasRemaining()) {
				written += channel.write(src);
			}
		}
		LOG.debug("wrote " + written + " bytes to server");

	}

	private void writeInt(int type) throws IOException {
		// write the request type to the server
		ByteBuffer srcType = ByteBuffer.allocate(4);
		srcType.putInt(type);
		srcType.flip();
		while (srcType.hasRemaining()) {
			channel.write(srcType);
			LOG.debug("wrote integer " + type + " to server");
		}
	}

	private void writeLong(long value) throws IOException {
		// write the request type to the server
		ByteBuffer srcType = ByteBuffer.allocate(8);
		srcType.putLong(value);
		srcType.flip();
		while (srcType.hasRemaining()) {
			channel.write(srcType);
			LOG.debug("wrote long " + value + " to server");
		}
	}
}
