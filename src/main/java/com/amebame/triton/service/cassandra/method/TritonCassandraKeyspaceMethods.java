package com.amebame.triton.service.cassandra.method;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.amebame.triton.client.cassandra.entity.TritonCassandraKeyspace;
import com.amebame.triton.client.cassandra.method.CreateKeyspace;
import com.amebame.triton.client.cassandra.method.DescribeKeyspace;
import com.amebame.triton.client.cassandra.method.DropKeyspace;
import com.amebame.triton.client.cassandra.method.ListKeyspace;
import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.cassandra.CassandraConverter;
import com.amebame.triton.service.cassandra.TritonCassandraClient;
import com.amebame.triton.service.cassandra.TritonCassandraException;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.SchemaChangeResult;

public class TritonCassandraKeyspaceMethods {
	
	private TritonCassandraClient client;

	@Inject
	public TritonCassandraKeyspaceMethods(TritonCassandraClient client) {
		this.client = client;
	}
	
	@TritonMethod("cassandra.keyspace.list")
	public List<TritonCassandraKeyspace> listKeyspaces(ListKeyspace data) {
		return CassandraConverter.toKeyspaceList(
				client.getKeyspaceDefinitions(
						data.getCluster()),
						true);
	}
	
	@TritonMethod("cassandra.keyspace.detail")
	public TritonCassandraKeyspace describeKeyspace(DescribeKeyspace describe) {
		return CassandraConverter.toKeyspace(
				client.getKeyspaceDefinition(
						describe.getCluster(),
						describe.getKeyspace()));
	}
	
	@TritonMethod("cassandra.keyspace.create")
	public boolean createKeyspace(CreateKeyspace create) {
		
		Keyspace keyspace = client.getKeyspace(create.getCluster(), create.getKeyspace());
		
		Map<String, Object> options = new HashMap<String, Object>();
		options.put("strategy_class", create.getStrategyClass());
		options.put("strategy_options", create.getStrategyOptions());
		options.put("durable_writes", create.isDurableWrites());
		
		try {
			OperationResult<SchemaChangeResult> result = keyspace.createKeyspace(options);
			return result != null;
		} catch (ConnectionException e) {
			throw new TritonCassandraException(TritonErrors.cassandra_connection_fail, e);
		}
		
	}
	
	@TritonMethod("cassandra.keyspace.drop")
	public boolean dropKeyspace(DropKeyspace drop) {
		
		Keyspace keyspace = client.getKeyspace(drop.getCluster(), drop.getKeyspace());
		
		try {
			OperationResult<SchemaChangeResult> result = keyspace.dropKeyspace();
			return result != null;
		} catch (ConnectionException e) {
			throw new TritonCassandraException(TritonErrors.cassandra_connection_fail, e);
		}
		
	}

}
