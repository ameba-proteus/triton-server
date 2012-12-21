package com.amebame.triton.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
	
	private String prefix;
	private AtomicInteger counter;

	public NamedThreadFactory(String prefix) {
		this.prefix = prefix;
		this.counter = new AtomicInteger();
	}

	@Override
	public Thread newThread(Runnable r) {
		int id = counter.incrementAndGet();
		Thread thread = new Thread(r);
		thread.setName(prefix + id);
		return thread;
	}

}
