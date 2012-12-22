package com.amebame.triton.protocol;

import java.util.Arrays;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.amebame.triton.json.Json;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Triton Message
 */
public class TritonMessage {
	
	public static final int HEADER_SIZE = 16;
	
	public static final short COMMAND = 0x0001;
	public static final short REPLY   = 0x0010;
	public static final short ERROR   = 0x0011;
	
	private static final byte[] RESERVED = new byte[6];
	static {
		Arrays.fill(RESERVED, (byte)0x00);
	}
	
	private short type;
	private int callId;
	
	private ChannelBuffer body;
	
	/**
	 * Create the triton message with custom ChannelBuffer.
	 * @param type
	 * @param callId
	 * @param body
	 */
	public TritonMessage(short type, int callId, ChannelBuffer body) {
		this.type = type;
		this.callId = callId;
		this.body = body;
	}
	
	/**
	 * Create the triton message with body object which can
	 * convert to JSON string.
	 * @param type
	 * @param callId
	 * @param body
	 */
	public TritonMessage(short type, int callId, Object body) {
		this.type = type;
		this.callId = callId;
		this.body = ChannelBuffers.wrappedBuffer(Json.bytes(body));
	}
	
	public boolean isCommand() {
		return type == COMMAND;
	}
	
	public boolean isError() {
		return type == ERROR;
	}
	
	public boolean isReply() {
		return type == REPLY;
	}
	
	/**
	 * Get command type
	 * @return
	 */
	public short getType() {
		return type;
	}
	
	/**
	 * Get length of the body
	 * @return
	 */
	public int getLength() {
		return body == null ? 0 : body.capacity();
	}
	
	/**
	 * Get call ID
	 * @return
	 */
	public int getCallId() {
		return callId;
	}
	
	/**
	 * Get body data
	 * @return
	 */
	public ChannelBuffer getBody() {
		return body;
	}
	
	/**
	 * Get body as data
	 * @param clazz
	 * @return
	 */
	public <E> E getBody(Class<E> clazz) {
		return Json.convert(body, clazz);
	}
	
	/**
	 * Get body as json tree
	 * @return
	 */
	public JsonNode getBodyJson() {
		return Json.tree(body);
	}
	
	/**
	 * Get expected frame length of the message.
	 * @return
	 */
	public int getFrameLength() {
		return HEADER_SIZE + getLength();
	}
	
	/**
	 * Write message to {@link ChannelBuffer}
	 * @param buffer
	 */
	public void writeTo(ChannelBuffer buffer) {
		buffer.writeShort(type);
		buffer.writeInt(getLength());
		buffer.writeInt(callId);
		buffer.writeBytes(RESERVED);
		buffer.writeBytes(body);
	}
}
