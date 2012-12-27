package com.amebame.triton.server;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.amebame.triton.service.TritonManagementMethods;

@Singleton
public class TritonServerSetup {

	@Inject
	public TritonServerSetup(
			TritonServerContext context,
			TritonManagementMethods heartbeat) {
		context.addServerMethod(heartbeat);
	}

}
