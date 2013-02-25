package org.hazelfs.services;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPService implements Runnable {
	public static final byte PROTOCOL_REQUEST_FILE = 0x01;
	public static final byte PROTOCOL_PUT_FILE = 0x02;
	public static final byte PROTOCOL_DELETE_FILE = 0x03;

	private static final Logger LOG = LoggerFactory.getLogger(TCPService.class);
	private final int port;
	private Selector selector;
	private ServerSocketChannel server;
	private boolean shutdown = false;

	public TCPService(int port) {
		this.port = port;
	}

	@Override
	public void run() {
		try {
			LOG.info("starting TCP service at port " + port);
			// init the NIO channels and sockets
			selector = Selector.open();
			server = ServerSocketChannel.open();
			server.socket().bind(new InetSocketAddress(port));
			server.configureBlocking(false);
			server.register(selector, SelectionKey.OP_ACCEPT);
		} catch (IOException e) {
			LOG.error("Unable to start TCP service on port " + port, e);
		}
		while (!shutdown) {
			try {
				if (selector.select() == 0) {
					Thread.sleep(100);
				} else {
					for (Iterator<SelectionKey> it = selector.selectedKeys().iterator(); it.hasNext();) {
						SelectionKey key = it.next();
						it.remove();
						if (key.isConnectable()) {
							((SocketChannel) key.channel()).finishConnect();
						}
						if (key.isAcceptable()) {
							SocketChannel channelIn = server.accept();
							channelIn.configureBlocking(false);
							channelIn.socket().setTcpNoDelay(true);
							channelIn.register(selector, SelectionKey.OP_READ);
						}
						if (key.isReadable()) {
							ByteBuffer buf = ByteBuffer.allocate(1);
							int numRead = ((ReadableByteChannel) key.channel()).read(buf);
							if (numRead > 0) {
								handleRequest(buf.get(0), key.channel());
							}
						}
					}
				}
			} catch (IOException | InterruptedException e) {
				LOG.error("Error while receiving data");
			}
		}
	}

	private void handleRequest(byte b, SelectableChannel channel) throws IOException {
		System.out.println("handling request 0x" + (int) b);
		switch (b) {
		case PROTOCOL_REQUEST_FILE:
			ByteBuffer buf = ByteBuffer.allocate(1024);
			int numRead;
			while ((numRead = ((ReadableByteChannel) channel).read(buf)) > 0) {
				for (int i =0;i<numRead;i++){
					System.out.println("recv: " + (char) buf.get(i) + " [0x" + (int) buf.get(i) + "]");
				}
			}
			break;
		default:
			break;
		}
	}

	public synchronized void shutdown() {
		this.shutdown = true;
	}
}
