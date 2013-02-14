package com.amebame.triton.service.cassandra.method;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Inject;

import com.amebame.triton.client.cassandra.entity.TritonCassandraBatchOperation;
import com.amebame.triton.client.cassandra.entity.TritonCassandraColumnFamily;
import com.amebame.triton.client.cassandra.method.BatchUpdate;
import com.amebame.triton.client.cassandra.method.CreateColumnFamily;
import com.amebame.triton.client.cassandra.method.DropColumnFamily;
import com.amebame.triton.client.cassandra.method.ListColumnFamily;
import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.cassandra.CassandraConverter;
import com.amebame.triton.service.cassandra.Serializers;
import com.amebame.triton.service.cassandra.TritonCassandraClient;
import com.amebame.triton.service.cassandra.TritonCassandraException;
import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
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
			throw new TritonCassandraException(TritonErrors.cassandra_connection_fail, e);
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
			throw new TritonCassandraException(TritonErrors.cassandra_connection_fail, e);
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
			throw new TritonCassandraException(TritonErrors.cassandra_connection_fail, e);
		}
	}
	
	/**
	 * Execute batch operations.
	 * @param batch
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@TritonMethod("cassandra.columnfamily.batch")
	public <K,C,V> boolean batch(BatchUpdate update) {
		
		try {
			
			// return if empty operations
			if (!update.hasOperations()) {
				return false;
			}
			
			// get the keyspace
			Keyspace keyspace = client.getKeyspace(update.getCluster(), update.getKeyspace());
			
			// prepare for atomic batch
			MutationBatch batch = keyspace.prepareMutationBatch();
			
			// set consistency
			if (update.getConsistency() != null) {
				batch.setConsistencyLevel(CassandraConverter.consistency(update.getConsistency()));
			}
			
			for (TritonCassandraBatchOperation operation : update.getOperations()) {
				
				String family = operation.getColumnFamily();
				ColumnFamily<K, C> columnFamily = (ColumnFamily<K, C>) client.getColumnFamily(
						update.getCluster(),
						update.getKeyspace(),
						family);
				
				Serializer<C> columnSerializer = columnFamily.getColumnSerializer();
				Serializer<K> keySerializer = columnFamily.getKeySerializer();
				Serializer<V> valueSerializer = (Serializer<V>) columnFamily.getDefaultValueSerializer();
				
				Map<String, Map<String, JsonNode>> updates = operation.getUpdates();
				Map<String, JsonNode> removes = operation.getRemoves();
				
				if (!(updates == null || updates.isEmpty())) {
					// execute updates
					for (Entry<String, Map<String, JsonNode>> rowEntry : updates.entrySet()) {
						String key = rowEntry.getKey();
						ColumnListMutation<C> mutation = batch.withRow(
								columnFamily,
								CassandraConverter.toObject(key, keySerializer)
								);
						for (Entry<String, JsonNode> columnEntry : rowEntry.getValue().entrySet()) {
							C column = CassandraConverter.toObject(columnEntry.getKey(), columnSerializer);
							if (operation.hasTtl()) {
								mutation.putColumn(
										column,
										CassandraConverter.toValueBuffer(
												columnEntry.getValue(),
												valueSerializer
										),
										operation.getTtl()
										);
							} else {
								mutation.putColumn(
										column,
										CassandraConverter.toValueBuffer(
												columnEntry.getValue(),
												valueSerializer
												)
										);
							}
						}
					}
				}
				
				if (!(removes == null || removes.isEmpty())) {
					// execute removes
					for (Entry<String, JsonNode> removeEntry : removes.entrySet()) {
						String key = removeEntry.getKey();
						JsonNode value = removeEntry.getValue();
						ColumnListMutation<C> mutation = batch.withRow(
								columnFamily,
								CassandraConverter.toObject(key, keySerializer));
						if (value.isArray()) {
							int size = value.size();
							for (int i = 0; i < size; i++) {
								String column = value.get(i).asText();
								mutation.deleteColumn(CassandraConverter.toObject(column, columnSerializer));
							}
						} else {
							mutation.delete();
						}
					}
				}
				
			}
			
			// execute atomic batches
			batch.execute();
			
		} catch (ConnectionException e) {
			throw new TritonCassandraException(TritonErrors.cassandra_connection_fail, e);
		}
		
		return true;
	}
	
}
