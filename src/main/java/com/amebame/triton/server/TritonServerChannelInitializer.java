package com.amebame.triton.server;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

import javax.inject.Inject;

import com.amebame.triton.protocol.TritonProtocolDecoder;
import com.amebame.triton.protocol.TritonProtocolEncoder;

public class TritonServerChannelInitializer extends ChannelInitializer<SocketChannel> {
	
	private TritonServerHandler handler;
	
	@Inject
	public TritonServerChannelInitializer(TritonServerHandler handler) {
		this.handler = handler;
	}
	
	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();
		pipeline.addLast("encoder", new TritonProtocolEncoder());
		pipeline.addLast("decoder", new TritonProtocolDecoder());
		pipeline.addLast("handler", handler);
	}

}
