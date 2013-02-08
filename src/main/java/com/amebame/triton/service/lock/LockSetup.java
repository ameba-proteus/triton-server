package com.amebame.triton.service.lock;

import javax.inject.Inject;

import com.amebame.triton.server.TritonServerContext;
import com.amebame.triton.service.TritonScheduler;
import com.amebame.triton.service.lock.method.LockMethods;

public class LockSetup {
	
	@Inject private TritonServerContext context;
	
	@Inject private LockManager manager;
	
	@Inject private LockMethods methods;
	
	@Inject private LockClearner cleaner;
	
	@Inject private TritonScheduler scheduler;

	public LockSetup() {
	}

	@Inject
	public void setup() {
		context.addServerMethod(methods);
		// execute context cleaning with 5 sec interval
		scheduler.scheduleWithFixedDelay(cleaner, 5000L, 5000L);
	}
}
