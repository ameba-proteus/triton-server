package com.amebame.triton.service;

import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;

import com.amebame.triton.server.TritonMethod;
import com.fasterxml.jackson.databind.JsonNode;

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
	 * Echo method return received json to the client
	 * @param text
	 * @return
	 */
	@TritonMethod("triton.echo")
	public JsonNode echo(JsonNode node) {
		return node;
	}
	
	/**
	 * This method let server to close client connection.
	 * This simulates closing from the server suddenly.
	 * @param channel
	 * @return
	 * @throws IOException
	 */
	@TritonMethod("triton.close")
	public void close(ChannelHandlerContext ctx) throws IOException {
		// close asynchronously
		ctx.close();
	}
}
