package com.amebame.triton.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.amebame.triton.config.TritonServerConfiguration;
import com.amebame.triton.util.NamedThreadFactory;

@Singleton
public class TritonServerContext {
	
	private TritonServerMethodMap methodMap;
	
	private ExecutorService executor;
	
	@Inject
	public TritonServerContext(TritonServerConfiguration config) {
		methodMap = new TritonServerMethodMap();
		executor = Executors.newFixedThreadPool(
				config.getNetty().getWorker(),
				new NamedThreadFactory("triton-worker-"));
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

	/**
	 * Get worker executor
	 * @return
	 */
	public ExecutorService getWorkerExecutor() {
		return executor;
	}
}
