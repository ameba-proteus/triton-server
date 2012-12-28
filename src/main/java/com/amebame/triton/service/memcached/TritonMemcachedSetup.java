package com.amebame.triton.service.memcached;

import javax.inject.Inject;

import com.amebame.triton.server.TritonServerCleaner;
import com.amebame.triton.server.TritonServerContext;
import com.amebame.triton.service.memcached.method.TritonMemcachedMethods;

public class TritonMemcachedSetup {
	
	@Inject private TritonServerCleaner cleaner;
	
	@Inject private TritonServerContext context;
	
	@Inject private TritonMemcachedClient client;
	
	@Inject private TritonMemcachedMethods methods;

	public TritonMemcachedSetup() {
	}

	@Inject
	public void setup() {
		cleaner.add(client);
		context.addServerMethod(methods);
	}
}
