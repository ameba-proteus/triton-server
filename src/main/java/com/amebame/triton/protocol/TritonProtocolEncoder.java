package com.amebame.triton.protocol;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * Triton Protocol Encoder
 * to encode data to TritonMesasge
 * 
 */
public class TritonProtocolEncoder extends OneToOneEncoder {
	
	@Override
	protected Object encode(
			ChannelHandlerContext ctx,
			Channel channel,
			Object object) throws Exception {
		
		// prevent non triton message
		if (!(object instanceof TritonMessage)) {
			return object;
		}
		TritonMessage message = (TritonMessage) object;
		
		// create buffer which can contain whole data
		ChannelBuffer buffer = ChannelBuffers.buffer(message.getFrameLength());
		// write to message
		message.writeTo(buffer);
		return buffer;
	}
}
