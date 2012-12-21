package com.amebame.triton.service.cassandra;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.cassandra.entity.CreateKeyspace;
import com.amebame.triton.service.cassandra.entity.DropKeyspace;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.SchemaChangeResult;

public class TritonCassandraMethods {
	
	private TritonCassandraClient client;

	@Inject
	public TritonCassandraMethods(TritonCassandraClient client) {
		this.client = client;
	}
	
	@TritonMethod("cassandra.keyspace.create")
	public boolean createKeyspace(CreateKeyspace create) {
		
		Keyspace keyspace = client.getKeyspace(create.getCluster(), create.getKeyspace());
		
		Map<String, Object> options = new HashMap<String, Object>();
		options.put("placement_strategy", create.getPlacementStrategy());
		options.put("strategy_options", create.getStrategyOptions());
		
		try {
			OperationResult<SchemaChangeResult> result = keyspace.createKeyspace(options);
			return result != null;
		} catch (ConnectionException e) {
			throw new TritonCassandraException(e.getMessage(), e);
		}
		
	}
	
	@TritonMethod("cassandra.keyspace.drop")
	public boolean dropKeyspace(DropKeyspace drop) {
		
		Keyspace keyspace = client.getKeyspace(drop.getCluster(), drop.getKeyspace());
		
		try {
			OperationResult<SchemaChangeResult> result = keyspace.dropKeyspace();
			return result != null;
		} catch (ConnectionException e) {
			throw new TritonCassandraException(e.getMessage(), e);
		}
		
	}

}
