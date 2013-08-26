package com.amebame.triton.server;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.amebame.triton.config.TritonServerConfiguration;

@Singleton
public class TritonServerContext {
	
	private TritonServerMethodMap methodMap;
	
	@Inject
	public TritonServerContext(TritonServerConfiguration config) {
		methodMap = new TritonServerMethodMap();
	}
	
	/**
	 * Add server methods from the target object
	 * @param target
	 */
	public void addServerMethod(Object target) {
		methodMap.register(target);
	}
	
	/**
	 * Get server method by name
	 * @param name
	 * @return
	 */
	public TritonServerMethod getServerMethod(String name) {
		return methodMap.getMethod(name);
	}
}
