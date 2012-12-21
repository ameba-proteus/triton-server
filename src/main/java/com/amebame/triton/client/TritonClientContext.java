package com.amebame.triton.client;

import com.amebame.triton.entity.TritonFuture;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

public class TritonClientContext {
	
	private TritonClientConfiguration config;
	
	private ConcurrentLinkedHashMap<Integer, TritonFuture> callmap;
	
	public TritonClientContext(TritonClientConfiguration config) {
		// call map exceed if it stores over 1000 items
		this.callmap = new ConcurrentLinkedHashMap.Builder<Integer, TritonFuture>()
				.maximumWeightedCapacity(10000)
				.build()
				;
		this.config = config;
	}
	
	public TritonClientConfiguration getConfig() {
		return config;
	}

	public void addFuture(TritonFuture future) {
		this.callmap.put(future.getCallId(), future);
	}
	
	public TritonFuture getFuture(int callId) {
		return callmap.get(callId);
	}
	
	public TritonFuture removeFuture(int callId) {
		return callmap.remove(callId);
	}
}
