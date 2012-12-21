package com.amebame.triton.server;

import javax.inject.Inject;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

import com.amebame.triton.protocol.TritonProtocolDecoder;
import com.amebame.triton.protocol.TritonProtocolEncoder;

public class TritonServerPipelineFactory implements ChannelPipelineFactory {
	
	private TritonServerHandler handler;
	
	@Inject
	public TritonServerPipelineFactory(TritonServerHandler handler) {
		this.handler = handler;
	}

	@Override
	public ChannelPipeline getPipeline() throws Exception {
		ChannelPipeline pipeline = Channels.pipeline();
		pipeline.addLast("encoder", new TritonProtocolEncoder());
		pipeline.addLast("decoder", new TritonProtocolDecoder());
		pipeline.addLast("handler", handler);
		return pipeline;
	}

}
