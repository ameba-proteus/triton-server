package com.amebame.triton.service.cassandra;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.amebame.triton.server.TritonServerCleaner;
import com.amebame.triton.server.TritonServerContext;
import com.amebame.triton.service.cassandra.method.TritonCassandraClusterMethods;
import com.amebame.triton.service.cassandra.method.TritonCassandraTableMethods;
import com.amebame.triton.service.cassandra.method.TritonCassandraColumnMethods;
import com.amebame.triton.service.cassandra.method.TritonCassandraKeyspaceMethods;

@Singleton
public class TritonCassandraSetup {
	
	@Inject private TritonServerCleaner cleaner;
	
	@Inject private TritonCassandraClient client;
	
	@Inject private TritonServerContext context;
	
	@Inject private TritonCassandraClusterMethods clusterMethods;
	
	@Inject private TritonCassandraKeyspaceMethods keyspaceMethods;
	
	@Inject private TritonCassandraTableMethods columnFamilyMethods;
	
	@Inject private TritonCassandraColumnMethods columnMethods;

	public TritonCassandraSetup() {
	}
	
	@Inject
	public void setup() {
		cleaner.add(client);
		context.addServerMethod(clusterMethods);
		context.addServerMethod(keyspaceMethods);
		context.addServerMethod(columnFamilyMethods);
		context.addServerMethod(columnMethods);
	}

}
