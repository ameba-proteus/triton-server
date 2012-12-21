package com.amebame.triton.server;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.amebame.triton.service.TritonHeartbeat;

@Singleton
public class TritonServerSetup {

	@Inject
	public TritonServerSetup(
			TritonServerContext context,
			TritonHeartbeat heartbeat) {
		context.addServerMethod(heartbeat);
	}

}
