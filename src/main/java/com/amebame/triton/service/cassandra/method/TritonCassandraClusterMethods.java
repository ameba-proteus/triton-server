package com.amebame.triton.service.cassandra.method;

import java.util.List;

import javax.inject.Inject;

import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.cassandra.TritonCassandraClient;

public class TritonCassandraClusterMethods {
	
	private TritonCassandraClient client;

	@Inject
	public TritonCassandraClusterMethods(TritonCassandraClient client) {
		this.client = client;
	}
	
	@TritonMethod("cassandra.cluster.list")
	public List<String> listCluster() {
		return client.getClusters();
	}
	
}
