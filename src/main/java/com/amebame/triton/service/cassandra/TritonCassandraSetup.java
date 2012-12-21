package com.amebame.triton.service.cassandra;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.amebame.triton.server.TritonServerCleaner;

@Singleton
public class TritonCassandraSetup {

	@Inject
	public TritonCassandraSetup(TritonServerCleaner cleaner, TritonCassandraClient client) {
		cleaner.add(client);
	}

}
