package com.amebame.triton.protocol;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

/**
 * Triton Protocol Encoder
 * to encode data to TritonMesasge
 * 
 */
public class TritonProtocolDecoder extends FrameDecoder {
	
	@Override
	protected Object decode(
			ChannelHandlerContext ctx,
			Channel channel,
			ChannelBuffer buffer) throws Exception {
		
		// Wait header data
		if (buffer.readableBytes() < 16) {
			return null;
		}
		
		buffer.markReaderIndex();
		
		// Read header
		short type = buffer.readShort();
		int length = buffer.readInt();
		int callId = buffer.readInt();
		
		// Skip reserved bytes
		buffer.skipBytes(6);
		
		// return if body has no enough data
		if (buffer.readableBytes() < length) {
			// reset position
			buffer.resetReaderIndex();
			return null;
		}
		
		// read body
		ChannelBuffer body = buffer.readBytes(length);
		
		TritonMessage message = new TritonMessage(type, callId, body);
		return message;
	}
}
