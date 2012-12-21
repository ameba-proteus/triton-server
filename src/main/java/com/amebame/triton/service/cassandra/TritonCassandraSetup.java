package com.amebame.triton.service.cassandra;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.amebame.triton.server.TritonServerCleaner;
import com.amebame.triton.server.TritonServerContext;

@Singleton
public class TritonCassandraSetup {

	@Inject
	public TritonCassandraSetup(
			TritonServerCleaner cleaner,
			TritonCassandraClient client,
			TritonServerContext context,
			TritonCassandraMethods methods) {
		cleaner.add(client);
		context.addServerMethod(methods);
	}

}
