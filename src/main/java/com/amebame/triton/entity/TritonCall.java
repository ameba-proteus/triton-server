package com.amebame.triton.entity;

import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.amebame.triton.json.Json;
import com.amebame.triton.protocol.TritonMessage;
import com.fasterxml.jackson.databind.JsonNode;

public class TritonCall {
	
	private static final AtomicInteger counter = new AtomicInteger();
	private static final int MAX_CALL_ID = Integer.MAX_VALUE / 2;
	
	private int callId;
	private TritonBody body;

	public TritonCall(String name, Object data) {
		this.callId = counter.incrementAndGet();
		if (this.callId > MAX_CALL_ID) {
			counter.compareAndSet(callId, 0);
		}
		
		JsonNode node = Json.tree(data);
		this.body = new TritonBody(name, node);
	}
	
	public int getCallId() {
		return callId;
	}
	
	public TritonBody getBody() {
		return body;
	}
	
	/**
	 * Build triton message from this object
	 * @return
	 */
	public TritonMessage build() {
		byte[] jsonByte = Json.bytes(body);
		ChannelBuffer bodyBuffer = ChannelBuffers.wrappedBuffer(jsonByte);
		return new TritonMessage(
				TritonMessage.COMMAND,
				callId,
				bodyBuffer
		);
	}
}
