package com.amebame.triton.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import com.amebame.triton.util.NamedThreadFactory;

@Singleton
public class TritonScheduler {
	
	private ScheduledExecutorService executor;

	public TritonScheduler() {
		executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("triton-scheduler-"));
	}

	/**
	 * Schedule once with delay
	 * @param command
	 * @param delay
	 */
	public void schedule(Runnable command, long delay) {
		executor.schedule(command, delay, TimeUnit.MILLISECONDS);
	}
	
	/**
	 * Schedule task at fixed rate
	 * @param command
	 * @param delay
	 * @param period
	 */
	public void scheduleAtFixedRate(Runnable command, long delay, long period) {
		executor.scheduleAtFixedRate(command, delay, period, TimeUnit.MILLISECONDS);
	}
	
	/**
	 * Schedule task with fixed delay
	 * @param command
	 * @param delay
	 * @param period
	 */
	public void scheduleWithFixedDelay(Runnable command, long delay, long period) {
		executor.scheduleWithFixedDelay(command, delay, period, TimeUnit.MILLISECONDS);
	}
	
}
