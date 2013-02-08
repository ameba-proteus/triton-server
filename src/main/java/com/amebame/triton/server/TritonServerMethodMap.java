package com.amebame.triton.server;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Triton Server Handlers
 * @author namura_suguru
 */
public class TritonServerMethodMap {
	
	private static final Logger log = LogManager.getLogger(TritonServerMethodMap.class);
	
	private Map<String, TritonServerMethod> map;

	public TritonServerMethodMap() {
		map = new HashMap<String, TritonServerMethod>();
	}

	/**
	 * Add server methods from the object
	 * @param object
	 */
	public void register(Object object) {
		log.info("adding server handler {}", object.getClass().getSimpleName());
		Method[] methods = object.getClass().getMethods();
		for (Method method : methods) {
			TritonMethod methodAnnotation = method.getAnnotation(TritonMethod.class);
			if (methodAnnotation != null) {
				TritonServerMethod tritonMethod = new TritonServerMethod(object, method, methodAnnotation);
				log.info("adding server method {}", methodAnnotation.value());
				map.put(methodAnnotation.value(), tritonMethod);
			}
		}
	}
	
	/**
	 * Get triton method from the name
	 * @param name
	 * @return
	 */
	public TritonServerMethod getMethod(String name) {
		return map.get(name);
	}
}
