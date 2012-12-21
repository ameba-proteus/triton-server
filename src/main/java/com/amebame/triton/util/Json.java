package com.amebame.triton.util;

import java.io.IOException;
import java.io.InputStream;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;

import com.amebame.triton.exception.TritonJsonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
			throw new TritonJsonException(e);
		}
	}
	
	/**
	 * Convert {@link JsonNode} to specific object instance
	 * @param node
	 * @param clazz
	 * @return
	 */
	public static final <E> E convert(JsonNode node, Class<E> clazz) {
		try {
			return MAPPER.treeToValue(node, clazz);
		} catch (JsonProcessingException e) {
			throw new TritonJsonException(e);
		}
	}
	
	/**
	 * Convert {@link ChannelBuffer} to specific object instance as JSON buffer.
	 * @param buffer
	 * @param clazz
	 * @return
	 */
	public static final <E> E convert(ChannelBuffer buffer, Class<E> clazz) {
		try {
			return MAPPER.readValue(new ChannelBufferInputStream(buffer), clazz);
		} catch (IOException e) {
			throw new TritonJsonException(e);
		}
	}
	
	/**
	 * Create an empty object node
	 * @return
	 */
	public static final ObjectNode object() {
		return JsonNodeFactory.instance.objectNode();
	}
	
	/**
	 * Create an empty array node
	 * @return
	 */
	public static final ArrayNode array() {
		return JsonNodeFactory.instance.arrayNode();
	}

}
