package com.amebame.triton.service.cassandra.method;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import com.amebame.triton.client.cassandra.entity.TritonCassandraCluster;
import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.cassandra.CassandraConverter;
import com.amebame.triton.service.cassandra.TritonCassandraClient;

public class TritonCassandraClusterMethods {
	
	private TritonCassandraClient client;

	@Inject
	public TritonCassandraClusterMethods(TritonCassandraClient client) {
		this.client = client;
	}
	
	@TritonMethod("cassandra.cluster.list")
	public List<TritonCassandraCluster> listCluster() {
		List<TritonCassandraCluster> list = new ArrayList<TritonCassandraCluster>();
		for (String name : client.getClusters()) {
			list.add(CassandraConverter.convertCluster(name));
		}
		return list;
	}
	
}
