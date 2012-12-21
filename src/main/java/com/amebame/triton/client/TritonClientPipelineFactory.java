package com.amebame.triton.client;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

import com.amebame.triton.protocol.TritonProtocolDecoder;
import com.amebame.triton.protocol.TritonProtocolEncoder;

public class TritonClientPipelineFactory implements ChannelPipelineFactory {
	
	private TritonClientContext context;
	
	public TritonClientPipelineFactory(TritonClientContext context) {
		this.context = context;
	}

	@Override
	public ChannelPipeline getPipeline() throws Exception {
		
		ChannelPipeline pipeline = Channels.pipeline();
		pipeline.addLast("encoder", new TritonProtocolEncoder());
		pipeline.addLast("decoder", new TritonProtocolDecoder());
		pipeline.addLast("handler", new TritonClientHandler(context));
		return pipeline;
		
	}

}
