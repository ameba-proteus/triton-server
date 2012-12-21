package com.amebame.triton.service.cassandra;

import javax.inject.Inject;

public class TritonCassandraMethods {
	
	private TritonCassandraClient client;

	@Inject
	public TritonCassandraMethods(TritonCassandraClient client) {
		this.client = client;
	}

}
