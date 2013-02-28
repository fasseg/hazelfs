package org.hazelfs.networking;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

import org.hazelfs.services.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelFSListener implements Callable<Integer> {

	private static final Logger LOG = LoggerFactory.getLogger(HazelFSListener.class);

	private final int port;
	private final StorageService storage;
	private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

	private boolean shutdown = false;
	private Selector selector;
	private Map<SocketAddress, Request> requests = new HashMap<SocketAddress, Request>();
	private Map<SocketAddress, Response> responses = new HashMap<SocketAddress, Response>();

	public HazelFSListener(int port, StorageService storage) {
		super();
		this.port = port;
		this.storage = storage;
	}

	@Override
	public Integer call() throws Exception {
		selector = initSelector();
		LOG.debug("Listener entering main loop");
		while (!shutdown) {

			if (selector.select() == 0) {
				LOG.debug("No keys...sleeping");
				Thread.sleep(77);
			}

			Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
			LOG.debug("Number of keys in selected set: " + selector.selectedKeys().size());
			while (keys.hasNext()) {
				SelectionKey key = keys.next();
				keys.remove();
				if (key.isAcceptable()) {
					acceptConnection(key);
				}
				if (key.isReadable()) {
					readRequest(key);
				}
				if (key.isWritable()) {
					writeResponse(key);
				}
			}
		}
		return -1;
	}

	private void writeResponse(SelectionKey key) throws IOException {
		SocketChannel client = (SocketChannel) key.channel();
		if (responses.containsKey(client.getRemoteAddress())) {
			Response resp = responses.get(client.getRemoteAddress());
			switch (resp.getState()) {
			case WRITE_STREAM_SIZE:
				writeStreamSize(client, resp);
				break;
			case WRITE_STREAM:
				if (resp.getStreamSize() == 0) {
					// a file was not found so we just return an error
					responses.remove(client.getRemoteAddress());
					client.register(selector, SelectionKey.OP_READ);
				}
				break;
			case WRITE_PUT_FILE_OK:
				writeInt(Constants.PROTO_CREATE_FILE, key, resp);
				responses.remove(client.getRemoteAddress());
				resp.getRequest().setState(Request.State.READ_STREAM_SIZE);
				resp.getRequest().setBuffer(ByteBuffer.allocate(8));
				requests.put(client.getRemoteAddress(), resp.getRequest());
				client.register(selector, SelectionKey.OP_READ);
				break;
			}
		}
	}

	private void writeInt(int value, SelectionKey key, Response resp) throws IOException {
		SocketChannel ch = (SocketChannel) key.channel();
		ByteBuffer buf = resp.getBuffer();
		buf.putInt(value);
		buf.flip();
		while (buf.hasRemaining()) {
			ch.write(buf);
		}
	}

	private void writeStreamSize(SocketChannel client, Response resp) throws IOException {
		ByteBuffer buf = resp.getBuffer();
		resp.setStreamSize(storage.getSize(resp.getRequest().getParam()));
		if (resp.getStreamSize() == 0) {
			LOG.warn("Unable to find file " + resp.getRequest().getParam());
		}
		buf.putLong(resp.getStreamSize());
		buf.flip();
		int numWritten = 0;
		while (numWritten < 8) {
			numWritten += client.write(buf);
		}
		resp.setState(Response.State.WRITE_STREAM);
		LOG.debug("wrote stream size " + resp.getStreamSize() + " to client " + client.getRemoteAddress());
	}

	private void readRequest(SelectionKey key) throws IOException {
		SocketChannel client = (SocketChannel) key.channel();
		if (requests.containsKey(client.getRemoteAddress())) {
			Request req = requests.get(client.getRemoteAddress());
			switch (req.getState()) {
			case READ_TYPE:
				readRequestType(client, req);
				break;
			case READ_PARAM_SIZE:
				readParamsSize(client, req);
				break;
			case READ_PARAM:
				readParam(client, req);
				break;
			case READ_STREAM_SIZE:
				readStreamSize(client, req);
				break;
			case READ_STREAM:
				readStream(client, req);
			}
		} else {
			Request req = new Request();
			requests.put(client.getRemoteAddress(), req);
		}
	}

	private void readStream(SocketChannel client, Request req) throws IOException {
		if (req.streamRead == 0){
			req.output = storage.create(req.getParam());
			LOG.debug("creating file " + req.getParam());
		}
		if (req.streamRead < req.streamSize) {
			ByteBuffer dst = req.getBuffer();
			int written = client.read(dst);
			req.streamRead+=written;
			dst.flip();
			while (dst.hasRemaining()){
				req.output.write(dst);
			}
		}
		if (req.streamRead == req.streamSize){
			client.register(selector, SelectionKey.OP_WRITE);
			req.setState(Request.State.READ_FINISHED);
		}
	}

	private void readParam(SocketChannel client, Request req) throws IOException {
		ByteBuffer buf = req.getBuffer();
		client.read(buf);
		if (!buf.hasRemaining()) {
			buf.flip();
			req.setParam(decoder.decode(buf).toString());
			req.setState(Request.State.READ_FINISHED);
			req.setBuffer(null);
			requests.remove(req);
			addResponse(client, req);
			client.register(selector, SelectionKey.OP_WRITE);
			LOG.debug("Request param " + req.getParam() + " with size " + req.getParamSize() + " from " + client.getRemoteAddress());
		}
	}

	private void addResponse(SocketChannel client, Request req) throws IOException {
		Response resp = new Response();
		if (req.getType() == Constants.PROTO_REQUEST_FILE) {
			resp.setState(Response.State.WRITE_STREAM_SIZE);
			resp.setBuffer(ByteBuffer.allocate(8));
		} else if (req.getType() == Constants.PROTO_CREATE_FILE) {
			resp.setState(Response.State.WRITE_PUT_FILE_OK);
			resp.setBuffer(ByteBuffer.allocate(4));
		}
		resp.setRequest(req);
		responses.put(client.getRemoteAddress(), resp);
	}

	private void readParamsSize(SocketChannel client, Request req) throws IOException {
		ByteBuffer buf = req.getBuffer();
		client.read(buf);
		if (!buf.hasRemaining()) {
			buf.flip();
			req.setParamSize(buf.getInt());
			req.setState(Request.State.READ_PARAM);
			req.setBuffer(ByteBuffer.allocate(req.getParamSize()));
			LOG.debug("Request param size " + req.getParamSize() + " from " + client.getRemoteAddress());
		}
	}

	private void readStreamSize(SocketChannel client, Request req) throws IOException {
		ByteBuffer buf = req.getBuffer();
		client.read(buf);
		if (!buf.hasRemaining()) {
			buf.flip();
			req.setStreamSize(buf.getLong());
			req.setState(Request.State.READ_STREAM);
			req.setBuffer(ByteBuffer.allocate(4096));
			LOG.debug("Request stream size " + req.getStreamSize() + " from " + client.getRemoteAddress());
		}
	}

	private void readRequestType(SocketChannel client, Request req) throws IOException {
		ByteBuffer buf = req.getBuffer();
		client.read(buf);
		if (!buf.hasRemaining()) {
			buf.flip();
			req.setType(buf.getInt());
			req.setState(Request.State.READ_PARAM_SIZE);
			buf.clear();
			req.setBuffer(buf);
			LOG.debug("Request type " + req.getType() + " from " + client.getRemoteAddress());
		}
	}

	private void acceptConnection(SelectionKey key) throws IOException {
		ServerSocketChannel sv = (ServerSocketChannel) key.channel();
		SocketChannel client = sv.accept();
		client.configureBlocking(false);
		client.register(selector, SelectionKey.OP_READ);
		LOG.debug("Accepting connection from " + client.getRemoteAddress());
	}

	private Selector initSelector() throws IOException {
		Selector selector = Selector.open();
		ServerSocketChannel server = ServerSocketChannel.open();
		server.socket().bind(new InetSocketAddress(port));
		server.configureBlocking(false);
		server.register(selector, SelectionKey.OP_ACCEPT);
		LOG.debug("Listener init finished");
		return selector;
	}

	public synchronized void shutdown() {
		LOG.debug("shutting down listener service on port " + port);
		this.shutdown = true;
	}

	private static class Response {
		public enum State {
			WRITE_STREAM_SIZE, WRITE_STREAM, WRITE_PUT_FILE_OK;
		}

		private FileChannel src;
		private Request request;
		private State state = State.WRITE_STREAM_SIZE;
		private ByteBuffer buffer = ByteBuffer.allocate(8);
		private long streamSize;
		private long bytesWritten;

		public State getState() {
			return state;
		}

		public long getStreamSize() {
			return streamSize;
		}

		public void setStreamSize(long streamSize) {
			this.streamSize = streamSize;
		}

		public long getBytesWritten() {
			return bytesWritten;
		}

		public void setBytesWritten(long bytesWritten) {
			this.bytesWritten = bytesWritten;
		}

		public void setState(State state) {
			this.state = state;
		}

		public FileChannel getSrc() {
			return src;
		}

		public void setSrc(FileChannel src) {
			this.src = src;
		}

		public Request getRequest() {
			return request;
		}

		public void setRequest(Request req) {
			this.request = req;
		}

		public ByteBuffer getBuffer() {
			return buffer;
		}

		public void setBuffer(ByteBuffer buffer) {
			this.buffer = buffer;
		}

	}

	private static class Request {
		public enum State {
			READ_TYPE, READ_PARAM_SIZE, READ_PARAM, READ_FINISHED, READ_STREAM, READ_STREAM_SIZE;
		}

		private int type;
		private int paramSize;
		private ByteBuffer buffer = ByteBuffer.allocate(4);
		private State state = State.READ_TYPE;
		private String param;
		private long streamSize;
		private long streamRead;
		private FileChannel output;

		public long getStreamRead() {
			return streamRead;
		}

		public void setStreamRead(long streamRead) {
			this.streamRead = streamRead;
		}


		public FileChannel getOutput() {
			return output;
		}

		public void setOutput(FileChannel output) {
			this.output = output;
		}

		public long getStreamSize() {
			return streamSize;
		}

		public void setStreamSize(long streamSize) {
			this.streamSize = streamSize;
		}

		public State getState() {
			return state;
		}

		public void setState(State state) {
			this.state = state;
		}

		public int getType() {
			return type;
		}

		public void setType(int type) {
			this.type = type;
		}

		public void setBuffer(ByteBuffer buffer) {
			this.buffer = buffer;
		}

		public String getParam() {
			return param;
		}

		public void setParam(String param) {
			this.param = param;
		}

		public ByteBuffer getBuffer() {
			return buffer;
		}

		public int getParamSize() {
			return paramSize;
		}

		public void setParamSize(int paramSize) {
			this.paramSize = paramSize;
		}

	}
}
