package org.hazelfs.services;

import java.io.IOException;
import java.io.InputStream;
import java.io.WriteAbortedException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.hazelfs.protocol.HazeResponse;
import org.hazelfs.protocol.HazelRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPListenerService implements Runnable {
	public static final byte PROTOCOL_REQUEST_FILE = 0x01;
	public static final byte PROTOCOL_PUT_FILE = 0x02;
	public static final byte PROTOCOL_DELETE_FILE = 0x03;
	public static final byte PROTOCOL_END_REQUEST = 0x00;

	private static final Logger LOG = LoggerFactory.getLogger(TCPListenerService.class);

	private final int port;
	private final StorageService storage;

	private Selector selector;
	private ServerSocketChannel server;
	private boolean shutdown = false;
	private Map<SocketAddress, TCPListenerService.Request> currentRequests = new HashMap<SocketAddress, TCPListenerService.Request>();

	protected TCPListenerService(int port, StorageService storage) {
		super();
		this.port = port;
		this.storage = storage;
	}

	@Override
	public void run() {
		// init the TCP server by binding a socket and registering a selector
		try {
			LOG.info("starting TCP service at port " + port);
			selector = Selector.open();
			server = ServerSocketChannel.open();
			server.socket().bind(new InetSocketAddress(port));
			server.configureBlocking(false);
			server.register(selector, SelectionKey.OP_ACCEPT);
		} catch (IOException e) {
			LOG.error("Unable to start TCP service on port " + port, e);
		}

		// the main loop
		try {
			while (!shutdown) {
				int numKeys = 0;
				numKeys = selector.select();
				if (numKeys == 0) {
					// nothing to do just wait for a bit and recheck
					Thread.sleep(89);
					continue;
				}
				Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
				while (keys.hasNext()) {
					SelectionKey key = keys.next();
					keys.remove();
					if (key.isAcceptable()) {
						// a new client connection has been opened
						SocketChannel client = server.accept();
						client.configureBlocking(false);
						client.socket().setTcpNoDelay(true);
						// register the client with the selector for read operations
						client.register(selector, SelectionKey.OP_READ);
						LOG.debug("accepted connection from " + client.getRemoteAddress().toString());
					} else if (key.isReadable()) {
						SocketChannel ch = (SocketChannel) key.channel();
						Request req;
						if (!currentRequests.containsKey(ch.getRemoteAddress())) {
							// create a new request object to be read
							req = new Request();
							// check the first byte indicating the type
							ByteBuffer buf = ByteBuffer.allocate(1);
							if (ch.read(buf) > 0) {
								if (buf.get(0) == PROTOCOL_REQUEST_FILE) {
									ParameterRequest preq = new ParameterRequest();
									preq.type = PROTOCOL_REQUEST_FILE;
									req = preq;
								} else if (buf.get(0) == PROTOCOL_PUT_FILE) {
									ParameterRequest preq = new ParameterRequest();
									preq.type = PROTOCOL_PUT_FILE;
									req = preq;
								} else {
									throw new IOException("Unknown protocol: " + buf.get(0));
								}
								currentRequests.put(ch.getRemoteAddress(), req);
							}
						}
						req = currentRequests.get(ch.getRemoteAddress());
						if (req instanceof ParameterRequest) {
							ParameterRequest param = (ParameterRequest) req;
							while (param.bytesRead < Integer.SIZE / Byte.SIZE) {
								ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE - param.bytesRead);
								int numRead = ch.read(sizeBuffer);
								for (int i = param.bytesRead; i < param.bytesRead + numRead; i++) {
									param.sizeBytes[i] = sizeBuffer.get(i);
								}
								param.bytesRead += numRead;
							}
							if (param.bytesRead == Integer.SIZE / Byte.SIZE) {
								param.size = ByteBuffer.wrap(param.sizeBytes).getInt(0);
								param.parameter = new byte[param.size];
							}
							while (param.bytesRead >= Integer.SIZE / Byte.SIZE && param.bytesRead + Integer.SIZE / Byte.SIZE < param.size) {
								ByteBuffer buf = ByteBuffer.allocate(param.size);
								int numRead = ch.read(buf);
								for (int i = param.bytesRead - Integer.SIZE / Byte.SIZE; i < param.size; i++) {
									param.parameter[i] = buf.get(i);
								}
								param.bytesRead += numRead;
							}
							if (param.bytesRead == param.size + Integer.SIZE / Byte.SIZE) {
								if (param.type == PROTOCOL_REQUEST_FILE) {
									GetFileRequest gf = new GetFileRequest();
									gf.path = new String(param.parameter);
									currentRequests.put(ch.getRemoteAddress(), gf);
								} else if (param.type == PROTOCOL_PUT_FILE) {
									PutFileRequest pf = new PutFileRequest();
									pf.path = new String(param.parameter);
									currentRequests.put(ch.getRemoteAddress(), pf);
								}
							}

						} else if (req instanceof GetFileRequest) {
							// fetch a file and provice it to the requester
							GetFileRequest gf = (GetFileRequest) req;
							gf.size = storage.getSize(gf.path);
							if (gf.size == 0) {
								// File not found, respond with an error
								currentRequests.put(ch.getRemoteAddress(), new ErrorRequest(HazeResponse.RETURN_ERROR));
								LOG.warn("error when fetching file " + gf.path + ": File not found");
							}
							LOG.debug("File " + gf.path + " has size " + gf.size);
							// register the client for writing
							ch.register(selector, SelectionKey.OP_WRITE);
						} else if (req instanceof PutFileRequest) {
							// get the size of of the file as a long value;
							PutFileRequest pf = (PutFileRequest) req;
							while (pf.bytesRead < Long.SIZE / Byte.SIZE) {
								ByteBuffer sizeBuf = ByteBuffer.allocate(Long.SIZE / Byte.SIZE - pf.bytesRead);
								int numRead = ch.read(sizeBuf);
								LOG.debug("accepting stream of size " + sizeBuf.getLong(0));
								for (int i = pf.bytesRead; i < Long.SIZE / Byte.SIZE; i++) {
									pf.sizeBytes[i] = sizeBuf.get(i - pf.bytesRead);
								}
								pf.bytesRead += numRead;
							}
							if (pf.bytesRead == Long.SIZE / Byte.SIZE) {
								pf.size = ByteBuffer.wrap(pf.sizeBytes).getLong();
								pf.sink = storage.create(pf.path);
							}
							while (pf.bytesRead >= Long.SIZE / Byte.SIZE && pf.bytesRead + Long.SIZE / Byte.SIZE <= pf.size) {
								ByteBuffer buf = ByteBuffer.allocate(4096);
								int numRead = ch.read(buf);
								int numWritten = 0;
								while (numWritten < numRead) {
									buf.flip();
									numWritten += pf.sink.write(buf, numWritten);
								}
								pf.bytesRead+=numRead;
							}
							if (pf.bytesRead == pf.size + Long.SIZE / Byte.SIZE) {
								// finished the put file, end the request
								pf.sink.close();
								currentRequests.put(ch.getRemoteAddress(), new SuccessRequest());
								ch.register(selector, SelectionKey.OP_WRITE);
							}
						}
					} else if (key.isWritable()) {
						SocketChannel ch = (SocketChannel) key.channel();
						if (currentRequests.containsKey(ch.getRemoteAddress())) {
							Request req = currentRequests.get(ch.getRemoteAddress());
							if (req instanceof ErrorRequest) {
								ByteBuffer error = ByteBuffer.wrap(new byte[] { HazeResponse.RETURN_ERROR });
								int numWritten = ch.write(error);
								if (numWritten == 1) {
									currentRequests.remove(ch.getRemoteAddress());
									ch.register(selector, SelectionKey.OP_READ);
								}
							}else if (req instanceof SuccessRequest){
								ByteBuffer error = ByteBuffer.wrap(new byte[] { HazeResponse.RETURN_OK });
								int numWritten = ch.write(error);
								if (numWritten == 1) {
									currentRequests.remove(ch.getRemoteAddress());
									ch.register(selector, SelectionKey.OP_READ);
								}
							}
						}
					}
				}
			}
		} catch (IOException | InterruptedException e) {
			LOG.error("Exception occured", e);
		}

	}

	public synchronized void shutdown() {
		this.shutdown = true;
	}

	private static class Request {
	}

	private static class SuccessRequest extends Request {
		private final byte code = HazeResponse.RETURN_OK;
	}

	private static class ErrorRequest extends Request {
		private final byte code;

		protected ErrorRequest(byte code) {
			super();
			this.code = code;
		}
	}

	private static class PutFileRequest extends Request {
		private String path;
		private long size;
		private int bytesRead;
		private byte[] sizeBytes = new byte[Long.SIZE / Byte.SIZE];
		private FileChannel sink;
	}

	private static class GetFileRequest extends Request {
		private long size;
		private String path;
		private long bytesWritten;
	}

	private static class ParameterRequest extends Request {
		private int type;
		private int bytesRead;
		private int size;
		private byte[] sizeBytes = new byte[Integer.SIZE / Byte.SIZE];
		private byte[] parameter;
	}
}
