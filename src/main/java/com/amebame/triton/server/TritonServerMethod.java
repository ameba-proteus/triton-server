package com.amebame.triton.server;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.jboss.netty.channel.Channel;

import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.exception.TritonException;
import com.amebame.triton.exception.TritonRuntimeException;
import com.amebame.triton.json.Json;
import com.amebame.triton.protocol.TritonMessage;
import com.fasterxml.jackson.databind.JsonNode;

public class TritonServerMethod {
	
	private Object object;
	
	private Method method;
	
	private Class<?>[] parameterTypes;
	
	private TritonMethod annotation;
	
	public TritonServerMethod(Object object, Method method, TritonMethod annotation) {
		this.object = object;
		this.method = method;
		this.parameterTypes = method.getParameterTypes();
		this.annotation = annotation;
	}
	
	public Object getObject() {
		return object;
	}
	
	public Method getMethod() {
		return method;
	}
	
	public boolean isSynchronous() {
		return !annotation.async();
	}
	
	public Object invoke(Channel channel, TritonMessage message, JsonNode body) {
		
		// set dynamic parameters
		try {
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
					if (body == null) {
						// empty object
						body = Json.object();
					}
					args[i] = Json.convert(body, parameterType);
				}
			}
			return method.invoke(object, args);
		} catch (InvocationTargetException e) {
			// get cause
			Throwable cause = e.getCause();
			int errorCode = TritonErrors.server_error.code();
			if (cause instanceof TritonException) {
				// get error code if exception if TritonException
				errorCode = ((TritonException) cause).getError().code();
			} else if (cause instanceof TritonRuntimeException) {
				// get error code from runtime exception
				errorCode = ((TritonRuntimeException) cause).getError().code();
			}
			// get root cause
			cause = ExceptionUtils.getRootCause(e);
			cause = cause == null ? e : cause;
			throw new TritonRuntimeException(TritonErrors.codeOf(errorCode), cause);
		} catch (IllegalAccessException | IllegalArgumentException e) {
			// throw as error
			throw new TritonRuntimeException(TritonErrors.server_error, e.getMessage());
		}
	}

}
