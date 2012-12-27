package com.amebame.triton.server;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.jboss.netty.channel.Channel;

import com.amebame.triton.exception.TritonRuntimeException;
import com.amebame.triton.json.Json;
import com.amebame.triton.protocol.TritonMessage;
import com.fasterxml.jackson.databind.JsonNode;

public class TritonServerMethod {
	
	private Object object;
	
	private Method method;
	
	private Class<?>[] parameterTypes;

	public TritonServerMethod(Object object, Method method) {
		this.object = object;
		this.method = method;
		this.parameterTypes = method.getParameterTypes();
	}
	
	public Object getObject() {
		return object;
	}
	
	public Method getMethod() {
		return method;
	}
	
	public Object invoke(Channel channel, TritonMessage message, JsonNode body) {
		
		// set dynamic parameters
		int length = parameterTypes.length;
		Object[] args = new Object[length];
		for (int i = 0; i < length; i++) {
			Class<?> parameterType = parameterTypes[i];
			if (parameterType == TritonMessage.class) {
				args[i] = message;
			} else if (parameterType == JsonNode.class) {
				args[i] = body;
			} else if (parameterType == Channel.class) {
				args[i] = channel;
			} else {
				args[i] = Json.convert(body, parameterType);
			}
		}
		try {
			return method.invoke(object, args);
		} catch (InvocationTargetException e) {
			// throw as generic error
			Throwable cause = e.getCause();
			throw new TritonRuntimeException(cause);
		} catch (IllegalAccessException | IllegalArgumentException e) {
			// throw as error
			throw new TritonRuntimeException(e.getMessage());
		}
		
	}

}
