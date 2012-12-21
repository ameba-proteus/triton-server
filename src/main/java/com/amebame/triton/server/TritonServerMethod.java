package com.amebame.triton.server;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amebame.triton.exception.TritonRuntimeException;

public class TritonServerMethod {
	
	private static final Logger log = LogManager.getLogger(TritonServerMethod.class);
	
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
	
	public Object invoke() {
		
		// set dynamic parameters
		int length = parameterTypes.length;
		Object[] args = new Object[length];
		for (int i = 0; i < length; i++) {
		}
		try {
			return method.invoke(object, args);
		} catch (InvocationTargetException e) {
			// throw as generic error
			Throwable cause = e.getCause();
			log.error(e.getMessage(), e);
			throw new TritonRuntimeException(cause.getMessage(), cause);
		} catch (IllegalAccessException | IllegalArgumentException e) {
			// throw as error
			log.error(e.getMessage(), e);
			throw new TritonRuntimeException(e.getMessage());
		}
		
	}

}
