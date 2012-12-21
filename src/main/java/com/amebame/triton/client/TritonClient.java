package com.amebame.triton.client;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.amebame.triton.entity.TritonCall;
import com.amebame.triton.entity.TritonFuture;
import com.amebame.triton.exception.TritonConnectException;
import com.amebame.triton.exception.TritonRuntimeException;
import com.amebame.triton.util.Json;
import com.fasterxml.jackson.databind.JsonNode;

public class TritonClient {
	
	private TritonClientContext context;
	private ChannelPipeline pipeline;
	private ChannelFactory channelFactory;
	private Channel channel;
	
	private static final JsonNode EMPTY_NODE = Json.object();
	
	public TritonClient() {
		this(new TritonClientConfiguration());
	}
	
	public TritonClient(TritonClientConfiguration config) {
		context = new TritonClientContext(config);
		channelFactory = new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool(),
				config.getNetty().getBoss(),
				config.getNetty().getWorker()
		);
		try {
			pipeline = new TritonClientPipelineFactory(context).getPipeline();
		} catch (Exception e) {
			throw new TritonRuntimeException(e.getMessage(), e);
		}
	}
	
	/**
	 * Opening connection to the server
	 * @param host
	 * @throws TritonConnectException
	 */
	public void open(String host) throws TritonConnectException {
		open(host, 4848);
	}
	
	/**
	 * Opening connection to the server
	 * @param host
	 * @param port
	 * @throws TritonConnectException
	 */
	public void open(String host, int port) throws TritonConnectException {
		channel = channelFactory.newChannel(pipeline);
		try {
			ChannelFuture future = channel.connect(new InetSocketAddress(host, port));
			if (!future.await(context.getConfig().getNetty().getConnectTimeout())) {
				throw new TritonConnectException("failed to connect to the server");
			}
		} catch (InterruptedException e) {
		}
	}
	
	/**
	 * Check client status.
	 * @return
	 */
	public boolean isOpen() {
		return channel != null && channel.isConnected();
	}
	
	/**
	 * send data to the server
	 * @param name
	 * @param data
	 */
	public void sendAsync(String name, Object data) {
		TritonCall call = new TritonCall(name, data);
		if (channel != null && channel.isOpen()) {
			channel.write(call.build());
		} else {
			// TODO error
		}
	}
	
	/**
	 * send only method to the server with future object.
	 * future will be invoked when server send reply
	 * @param name
	 * @param data
	 * @return
	 */
	public TritonFuture send(String name) {
		return send(name, EMPTY_NODE);
	}
	
	/**
	 * send data to the server with future object.
	 * future will be invoked when server send reply
	 * @param name
	 * @param data
	 * @return
	 */
	public TritonFuture send(String name, Object data) {
		if (channel != null && channel.isOpen()) {
			TritonCall call = new TritonCall(name, data);
			TritonFuture future = new TritonFuture(call);
			context.addFuture(future);
			// send message to the server
			channel.write(call.build());
			return future;
		} else {
			// TODO Handle not connected
			return null;
		}
	}

	/**
	 * Close the client connection
	 */
	public void close() {
		if (channel != null && channel.isConnected()) {
			try {
				channel.close().await(5000L);
			} catch (InterruptedException e) {
			}
		}
		channelFactory.shutdown();
	}
}
