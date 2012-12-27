package com.amebame.triton.service.cassandra.method;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.amebame.triton.client.cassandra.entity.TritonCassandraColumnFamily;
import com.amebame.triton.client.cassandra.method.CreateColumnFamily;
import com.amebame.triton.client.cassandra.method.DropColumnFamily;
import com.amebame.triton.client.cassandra.method.ListColumnFamily;
import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.cassandra.CassandraConverter;
import com.amebame.triton.service.cassandra.Serializers;
import com.amebame.triton.service.cassandra.TritonCassandraClient;
import com.amebame.triton.service.cassandra.TritonCassandraException;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.model.ColumnFamily;

public class TritonCassandraColumnFamilyMethods {
	
	private TritonCassandraClient client;

	@Inject
	public TritonCassandraColumnFamilyMethods(TritonCassandraClient client) {
		this.client = client;
	}

	/**
	 * Create the column family
	 * @param create
	 * @return
	 */
	@TritonMethod("cassandra.columnfamily.create")
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean createColumnFamily(CreateColumnFamily create) {
		
		Keyspace keyspace = client.getKeyspace(create.getCluster(), create.getKeyspace());
		
		Serializer<?> keySerializer = Serializers.get(create.getKeyValidationClass());
		Serializer<?> columnSerializer = Serializers.get(create.getComparator());
		Serializer<?> valueSerializer = Serializers.get(create.getDefaultValidationClass());
		
		ColumnFamily cf = new ColumnFamily(create.getColumnFamily(), keySerializer, columnSerializer, valueSerializer);
		
		Map<String, Object> options = new HashMap<String, Object>();
	
		try {
			OperationResult<SchemaChangeResult> result = keyspace.createColumnFamily(cf, options);
			return result != null;
		} catch (ConnectionException e) {
			throw new TritonCassandraException(e);
		}
	}
	
	/**
	 * Drop the column family
	 * @param drop
	 * @return
	 */
	@TritonMethod("cassandra.columnfamily.drop")
	public boolean dropColumnFamily(DropColumnFamily drop) {
		Keyspace keyspace = client.getKeyspace(drop.getCluster(), drop.getKeyspace());
		try {
			OperationResult<SchemaChangeResult> result = keyspace.dropColumnFamily(drop.getColumnFamily());
			return result != null;
		} catch (ConnectionException e) {
			throw new TritonCassandraException(e);
		}
	}
	
	/**
	 * List column family definitions
	 * @return
	 */
	@TritonMethod("cassandra.columnfamily.list")
	public List<TritonCassandraColumnFamily> listColumnFamily(ListColumnFamily list) {
		Keyspace keyspace = client.getKeyspace(list.getCluster(), list.getKeyspace());
		try {
			return CassandraConverter.toColumnFamilyList(
					keyspace.describeKeyspace().getColumnFamilyList()
			);
		} catch (ConnectionException e) {
			throw new TritonCassandraException(e);
		}
	}
	
}
