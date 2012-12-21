package com.amebame.triton.client;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.amebame.triton.entity.TritonFuture;
import com.amebame.triton.protocol.TritonMessage;

public class TritonClientHandler extends SimpleChannelUpstreamHandler {
	
	private TritonClientContext context;
	
	public TritonClientHandler(TritonClientContext context) {
		this.context = context;
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		TritonMessage message = (TritonMessage) e.getMessage();
		if (message.isReply() || message.isError()) {
			TritonFuture future = context.removeFuture(message.getCallId());
			if (future != null) {
				future.setResult(message);
			}
		}
	}

}
