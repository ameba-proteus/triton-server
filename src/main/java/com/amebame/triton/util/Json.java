package com.amebame.triton.util;

import java.io.IOException;
import java.io.InputStream;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;

import com.amebame.triton.exception.TritonJsonException;
import com.amebame.triton.exception.TritonRuntimeException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Json {
	
	private static final ObjectMapper MAPPER = new ObjectMapper();
	
	/**
	 * Get common {@link ObjectMapper}
	 * @return
	 */
	public static final ObjectMapper mapper() {
		return MAPPER;
	}
	
	public static final <E> E read(InputStream input, Class<E> clazz) {
		try {
			return MAPPER.readValue(input, clazz);
		} catch (IOException e) {
			throw new TritonJsonException(e.getMessage(), e);
		}
	}
	
	/**
	 * Get the json tree represents the target object
	 * @param target
	 * @return
	 */
	public static final JsonNode tree(Object target) {
		return MAPPER.valueToTree(target);
	}
	
	/**
	 * Get the json tree from channel buffer
	 * @param buffer
	 * @return
	 */
	public static final JsonNode tree(ChannelBuffer buffer) {
		try {
			return MAPPER.readTree(new ChannelBufferInputStream(buffer));
		} catch (IOException e) {
			throw new TritonJsonException(e.getMessage(), e);
		}
	}
	
	/**
	 * Convert objecdt to json bytes
	 * @param target
	 * @return
	 */
	public static final byte[] bytes(Object target) {
		try {
			return MAPPER.writeValueAsBytes(target);
		} catch (JsonProcessingException e) {
			throw new TritonRuntimeException(e);
		}
	}

}
