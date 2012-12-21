package com.amebame.triton.server;

import javax.inject.Singleton;

@Singleton
public class TritonServerContext {
	
	private TritonServerMethodMap methodMap;
	
	public TritonServerContext() {
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
