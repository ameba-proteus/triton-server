package com.amebame.triton.service;

import java.io.IOException;

import org.jboss.netty.channel.Channel;

import com.amebame.triton.server.TritonMethod;

/**
 * {@link TritonManagementMethods} has
 * some methods called by client which
 * get information about the server.
 * This class also provides some control methods.
 */
public class TritonManagementMethods {

	public TritonManagementMethods() {
	}

	/**
	 * Ping method respond pong with server clock.
	 * @return
	 */
	@TritonMethod("triton.heartbeat")
	public long ping() {
		return (int) (System.currentTimeMillis() / 1000L);
	}
	
	/**
	 * Echo method return received text to the client
	 * @param text
	 * @return
	 */
	@TritonMethod("triton.echo")
	public String echo(String text) {
		return text;
	}
	
	/**
	 * This method let server to close client connection.
	 * This simulates closing from the server suddenly.
	 * @param channel
	 * @return
	 * @throws IOException
	 */
	@TritonMethod("triton.close")
	public void close(Channel channel) throws IOException {
		// close asynchronously
		channel.close();
	}
}
