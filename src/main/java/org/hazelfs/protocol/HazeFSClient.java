package org.hazelfs.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.hazelfs.services.TCPListenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazeFSClient {

	private static final Logger LOG = LoggerFactory.getLogger(HazeFSClient.class);

	private final String host;
	private final int port;
	private final Socket socket;

	public HazeFSClient(String host, int port) {
		this.host = host;
		this.port = port;
		socket = new Socket();
	}

	public void setSocketTimeout(int millis) throws SocketException {
		this.socket.setSoTimeout(millis);
	}

	public void connect() throws IOException {
		socket.connect(new InetSocketAddress(host, port));
	}

	public void disconnect() throws IOException {
		socket.close();
	}

	public HazeResponse execute(HazelRequest req) throws IOException {
		if (req.getType() == HazelRequest.REQUEST_GET) {
			HazeGet get = (HazeGet) req;
			return requestFile(get.getPath());
		}
		if (req.getType() == HazelRequest.REQUEST_PUT) {
			HazePut put = (HazePut) req;
			return putFile(put.getPath(), put.getData(), put.getSize());
		}
		throw new IOException("Unabel to handle request of type " + req.getType());
	}

	private HazeResponse putFile(String path, InputStream data, long size) throws IOException {
		socket.getOutputStream().write(TCPListenerService.PROTOCOL_PUT_FILE);
		ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.SIZE/Byte.SIZE)
				.putInt(path.getBytes().length);
		socket.getOutputStream().write(sizeBuffer.array());
		socket.getOutputStream().write(path.getBytes());
		sizeBuffer = ByteBuffer.allocate(Long.SIZE/Byte.SIZE)
				.putLong(size);
		socket.getOutputStream().write(sizeBuffer.array());
		socket.getOutputStream().flush();
		IOUtils.copy(data, socket.getOutputStream());
		byte returnCode = (byte) socket.getInputStream().read();
		if (returnCode == HazeResponse.RETURN_ERROR) {
			socket.getOutputStream().close();
			throw new IOException("Unable to put file " + path + ". HazeFS returned " + returnCode);
		}
		socket.getOutputStream().close();
		return new HazeResponse(returnCode, null);
	}

	private HazeResponse requestFile(String path) throws IOException {
		LOG.debug("requesting " + path + " with length " + path.getBytes().length);
		socket.getOutputStream().write(TCPListenerService.PROTOCOL_REQUEST_FILE);
		ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.SIZE/Byte.SIZE)
				.putInt(path.getBytes().length);
		socket.getOutputStream().write(sizeBuffer.array());
		socket.getOutputStream().write(path.getBytes());
		socket.getOutputStream().write(TCPListenerService.PROTOCOL_END_REQUEST);
		socket.getOutputStream().flush();
		byte returnCode = (byte) socket.getInputStream().read();
		if (returnCode == HazeResponse.RETURN_ERROR) {
			socket.getOutputStream().close();
			throw new IOException("HazeFS returned " + returnCode);
		}
		socket.getOutputStream().close();
		return new HazeResponse(returnCode, socket.getInputStream());
	}
}
