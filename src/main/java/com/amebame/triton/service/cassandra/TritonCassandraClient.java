package com.amebame.triton.service.cassandra;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.inject.Inject;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.utils.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.amebame.triton.client.cassandra.method.GetColumns;
import com.amebame.triton.client.cassandra.method.RemoveColumns;
import com.amebame.triton.client.cassandra.method.SetColumns;
import com.amebame.triton.config.TritonCassandraClusterConfiguration;
import com.amebame.triton.config.TritonCassandraConfiguration;
import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.server.TritonCleaner;
import com.amebame.triton.server.util.BytesUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.UnknownComparatorException;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.ByteBufferRangeImpl;

/**
 * Client for cassandra clusters
 */
public class TritonCassandraClient implements TritonCleaner {
	
	private static final Logger log = LogManager.getLogger(TritonCassandraClient.class);
	
	private ConcurrentMap<String, ClusterHolder> clustermap;
	
	private ConcurrentMap<String, KeyspaceHolder> keyspacemap;
	
	private ConcurrentMap<String, ColumnFamily<?, ?>> cfmap;
	
	private TritonCassandraConfiguration config;
	
	private Lock lock;
	
	private static final Integer DEFAULT_LIMIT_ROWS = 100;
	private static final Integer DEFAULT_LIMIT_COLUMNS = 1000;
	private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
	
	@Inject
	public TritonCassandraClient(TritonCassandraConfiguration config) {
		this.clustermap = new ConcurrentHashMap<>();
		this.keyspacemap = new ConcurrentHashMap<>();
		this.cfmap = new ConcurrentHashMap<>();
		this.lock = new ReentrantLock();
		this.config = config;
		log.info("initialized triton cassandra client");
	}
	
	/**
	 * Get cluster list
	 * @return
	 */
	public List<String> getClusters() {
		List<String> list = new ArrayList<>();
		list.addAll(config.getClusters().keySet());
		return list;
	}
	
	/**
	 * Get the cluster by name
	 * @param clusterName
	 * @return
	 */
	public Cluster getCluster(String clusterName) {
		return getClusterHolder(clusterName).getCluster();
	}
	
	/**
	 * Get the partitioner by name
	 * @param clusterName
	 * @return
	 */
	public IPartitioner<? extends Token<?>> getPartitioner(String clusterName) {
		return getClusterHolder(clusterName).getPartitioner();
	}
	
	/**
	 * Get keyspace list
	 * @param cluster
	 * @return
	 */
	public List<KeyspaceDefinition> getKeyspaceDefinitions(String clusterName) {
		try {
			return getCluster(clusterName).describeKeyspaces();
		} catch (ConnectionException e) {
			throw new TritonCassandraException(TritonErrors.cassandra_connection_fail, e);
		}
	}
	
	/**
	 * Get keyspace definition
	 * @param clusterName
	 * @param keyspaceName
	 * @return
	 */
	public KeyspaceDefinition getKeyspaceDefinition(String clusterName, String keyspaceName) {
		try {
			return getKeyspace(clusterName, keyspaceName).describeKeyspace();
		} catch (ConnectionException e) {
			throw new TritonCassandraException(TritonErrors.cassandra_connection_fail, e);
		}
	}
	
	/**
	 * Get ring list of the keyspace
	 * @param clusterName
	 * @param keyspaceName
	 * @return
	 */
	public List<TokenRange> getTokenRange(String clusterName, String keyspaceName) {
		try {
			return getKeyspace(clusterName, keyspaceName).describeRing();
		} catch (ConnectionException e) {
			throw new TritonCassandraException(TritonErrors.cassandra_connection_fail, e);
		}
	}
	
	/**
	 * Get keyspace instance
	 * @param clusterName
	 * @param keyspaceName
	 * @return
	 */
	public Keyspace getKeyspace(String clusterName, String keyspaceName) {
		return getKeyspaceContext(clusterName, keyspaceName).getEntity();
	}
	
	/**
	 * Get keyspace context
	 * @param clusterName
	 * @param keyspaceName
	 * @return
	 */
	public AstyanaxContext<Keyspace> getKeyspaceContext(String clusterName, String keyspaceName) {
		return getKeyspaceHolder(clusterName, keyspaceName).getContext();
	}
	
	/**
	 * Get cluster holder
	 * @param clusterName
	 * @return
	 */
	private ClusterHolder getClusterHolder(String clusterName) {
		ClusterHolder holder = clustermap.get(clusterName);
		if (holder == null) {
			lock.lock();
			try {
				holder = clustermap.get(clusterName);
				if (holder == null) {
					log.info("creating cassandra cluter context for {}", clusterName);
					TritonCassandraClusterConfiguration clusterConfig = getClusterConfig(clusterName);
					// build the astyanax context
					AstyanaxContext<Cluster> context = new AstyanaxContext.Builder()
					.forCluster(clusterConfig.getName())
					.withAstyanaxConfiguration(clusterConfig.getAstyanaxConfig())
					.withConnectionPoolConfiguration(
							new ConnectionPoolConfigurationImpl(clusterName+"-pool")
							.setMaxConnsPerHost(1)
							.setSeeds(clusterConfig.getPoolConfig().getSeeds())
					)
					.buildCluster(ThriftFamilyFactory.getInstance());
					context.start();
					holder = new ClusterHolder(context);
					clustermap.put(clusterName, holder);
				}
			} finally {
				lock.unlock();
			}
		}
		return holder;
	}
	
	/**
	 * Get keyspace holder
	 * @param clusterName
	 * @param keyspaceName
	 * @return
	 */
	private KeyspaceHolder getKeyspaceHolder(String clusterName, String keyspaceName) {
		String key = getKey(clusterName, keyspaceName);
		KeyspaceHolder holder = keyspacemap.get(key);
		if (holder == null) {
			lock.lock();
			try {
				holder = keyspacemap.get(key);
				if (holder == null) {
					log.info("creating keyspace context for {} in {}", keyspaceName, clusterName);
					TritonCassandraClusterConfiguration clusterConfig = getClusterConfig(clusterName);
					// build the astyanax context
					AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
					.forCluster(clusterConfig.getName())
					.forKeyspace(keyspaceName)
					.withAstyanaxConfiguration(clusterConfig.getAstyanaxConfig())
					.withConnectionPoolConfiguration(clusterConfig.getPoolConfig())
					.buildKeyspace(ThriftFamilyFactory.getInstance());
					context.start();
					holder = new KeyspaceHolder(context);
					keyspacemap.put(key, holder);
				}
			} finally {
				lock.unlock();
			}
		}
		return holder;
	}
	
	/**
	 * Get {@link ColumnFamilyInfo}. It will update keyspace definition if not exists.
	 * @param clusterName
	 * @param keyspaceName
	 * @param columnFamilyName
	 * @return
	 */
	public ColumnFamily<?, ?> getColumnFamily(String clusterName, String keyspaceName, String columnFamilyName) {
		String key = getKey(clusterName, keyspaceName, columnFamilyName);
		ColumnFamily<?, ?> cf = cfmap.get(key);
		if (cf == null) {
			updateKeyspaceDefinition(clusterName, keyspaceName);
			cf = cfmap.get(key);
			if (cf == null) {
				throw new TritonCassandraException(
						TritonErrors.cassandra_no_column_family,
						"column family does not exists for " + columnFamilyName + " in " + keyspaceName);
			}
		}
		return cf;
	}
	
	private void updateKeyspaceDefinition(String clusterName, String keyspaceName) {
		try {
			log.info("updating keyspace definition for {} in {}", keyspaceName, clusterName);
			AstyanaxContext<Keyspace> context = getKeyspaceContext(clusterName, keyspaceName);
			Keyspace keyspace = context.getEntity();
			KeyspaceDefinition definition = keyspace.describeKeyspace();
			// read all column families
			for (ColumnFamilyDefinition cfdef : definition.getColumnFamilyList()) {
				String key = getKey(clusterName, keyspaceName, cfdef.getName());
				SerializerPackage sp = keyspace.getSerializerPackage(cfdef.getName(), true);
				ColumnFamily<?, ?> cf = new ColumnFamily<>(
						cfdef.getName(),
						sp.getKeySerializer(),
						sp.getColumnNameSerializer(),
						sp.getDefaultValueSerializer());
				cfmap.put(key, cf);
			}
		} catch (ConnectionException e) {
			throw new TritonCassandraException(TritonErrors.cassandra_connection_fail, e);
		} catch (UnknownComparatorException e) {
			throw new TritonCassandraException(TritonErrors.cassandra_invalid_comparator, e);
		}
	}
	
	/**
	 * Get default builder for both cluster and keyspace.
	 * @param clusterName
	 * @return
	 */
	private TritonCassandraClusterConfiguration getClusterConfig(String clusterName) {
		TritonCassandraClusterConfiguration clusterConfig = config.getCluster(clusterName);
		// return null if cluster is not defined
		if (clusterConfig == null) {
			throw new TritonCassandraException(
					TritonErrors.cassandra_no_cluster,
					"cluster has not been configured for " + clusterName);
		}
		return clusterConfig;
	}

	/**
	 * Construct key for the keyspace
	 * @param cluster
	 * @param keyspace
	 * @return
	 */
	private String getKey(String cluster, String keyspace) {
		return new StringBuilder(32)
		.append(cluster)
		.append('.')
		.append(keyspace)
		.toString();
	}
	
	/**
	 * Construct key for the column family
	 * @param cluster
	 * @param keyspace
	 * @param columnfamily
	 * @return
	 */
	private String getKey(String cluster, String keyspace, String columnfamily) {
		return new StringBuilder(48)
		.append(cluster)
		.append('.')
		.append(keyspace)
		.append('.')
		.append(columnfamily)
		.toString();
	}
	
	/**
	 * Get columns by {@link GetColumns} parameter.
	 */
	@SuppressWarnings("unchecked")
	public <K,C> Object getColumns(GetColumns gets) {
		
		Keyspace keyspace = getKeyspace(gets.getCluster(), gets.getKeyspace());
		ColumnFamily<K, C> cf = (ColumnFamily<K, C>) getColumnFamily(
				gets.getCluster(),
				gets.getKeyspace(),
				gets.getColumnFamily()
		);
		Serializer<K> keySerializer = cf.getKeySerializer();
		Serializer<C> columnSerializer = cf.getColumnSerializer();
		Serializer<?> valueSerializer = cf.getDefaultValueSerializer();
		
		try {
			ColumnFamilyQuery<K, C> query = keyspace.prepareQuery(cf);
			if (gets.getConsitency() != null) {
				query.setConsistencyLevel(CassandraConverter.consistency(gets.getConsitency()));
			}
			if (gets.isSingleKey()) {
				// Single key query
				K key = CassandraConverter.toObject(gets.getKeys().asText(), keySerializer);
				RowQuery<K, C> row = query.getKey(key);
				Map<String, JsonNode> map;
				List<CassandraColumn<C>> list;
				if (gets.hasColumns()) {
					JsonNode columns = gets.getColumns();
					if (gets.hasColumnRange()) {
						// set range query
						ByteBufferRange range = createRange(columns, columnSerializer);
						row.withColumnRange(range);
					} else {
						// set columns
						List<C> slice = CassandraConverter.toObjectList(gets.getColumns(), columnSerializer);
						row.withColumnSlice(slice);
					}
				}
				// get column list
				ColumnList<C> columns = row.execute().getResult();
				if (gets.hasSingleColumn()) {
					if (columns.size() <= 0) {
						return null;
					}
					Column<C> column = columns.getColumnByIndex(0);
					if (column == null) {
						return null;
					}
					return CassandraConverter.toValueNode(
							column.getByteArrayValue(),
							valueSerializer
					);
				} else if (gets.hasColumnRange()) {
					list = new ArrayList<>(columns.size());
					for (Column<C> column : columns) {
						// add to list
						JsonNode valueNode = CassandraConverter.toValueNode(
								column.getByteArrayValue(),
								valueSerializer);
						list.add(new CassandraColumn<C>(
								column.getName(),
								valueNode,
								column.getTimestamp()
								));
					}
					return list;
				} else {
					if (columns.size() == 0) {
						return null;
					}
					map = new HashMap<>(columns.size());
					for (Column<C> column : columns) {
						// add to map
						String keyText = columnSerializer.getString(column.getRawName());
						JsonNode valueNode = CassandraConverter.toValueNode(
								column.getByteArrayValue(),
								valueSerializer);
						map.put(keyText, valueNode);
					}
					return map;
				}
				
			} else {
				
				// multiple key or range key query
				
				RowSliceQuery<K, C> slice = null;
				if (gets.hasKeyArray()) {
					// Multiple key query
					List<K> keys = CassandraConverter.toObjectList(
							gets.getKeys(),
							cf.getKeySerializer());
					slice = query.getKeySlice(keys);
				} else if (gets.hasKeyRange()) {
					// Range key query
					// get the partitioner to calculate tokens
					IPartitioner<? extends Token<?>> partitioner = getPartitioner(gets.getCluster());
					// resolve token ranges
					String startToken = null;
					String endToken = null;
					// get keys node
					JsonNode keys = gets.getKeys();
					// start token
					startToken = getRangeToken(keys.get("start"), keySerializer, partitioner, true);
					endToken = getRangeToken(keys.get("end"), keySerializer, partitioner, false);
					// limit default 100
					int limit = DEFAULT_LIMIT_ROWS;
					if (keys.has("limit")) {
						limit = keys.get("limit").asInt();
					}
					// token range query
					slice = query
							.getRowRange(null, null, startToken, endToken, limit)
							;
				} else {
					IPartitioner<? extends Token<?>> partitioner = getPartitioner(gets.getCluster());
					Token<?> minimumToken = partitioner.getMinimumToken();
					String token = partitioner.getTokenFactory().toString(minimumToken);
					slice = query.getRowRange(null, null, token, token, DEFAULT_LIMIT_ROWS);
				}
				if (gets.hasColumns()) {
					JsonNode columns = gets.getColumns();
					if (gets.hasColumnRange()) {
						// set range query
						ByteBufferRange range = createRange(columns, columnSerializer);
						slice.withColumnRange(range);
					} else {
						// set columns
						List<C> list = CassandraConverter.toObjectList(gets.getColumns(), columnSerializer);
						slice.withColumnSlice(list);
					}
				}
				// get slice rows
				Rows<K,C> rows = slice.execute().getResult();
				
				// convert result to adaptive types
				if (gets.hasKeyRange()) {
					// row as array if key range specified
					List<CassandraRow<C>> list = new ArrayList<>();
					if (gets.hasColumnRange()) {
						for (Row<K, C> row : rows) {
							ColumnList<C> columns = row.getColumns();
							if (!columns.isEmpty()) {
								list.add(new CassandraRow<C>(
										CassandraConverter.toString(row.getKey(), keySerializer),
										CassandraConverter.toCassandraColumnList(columns, columnSerializer)
										));
							}
						}
					} else {
						for (Row<K, C> row : rows) {
							ColumnList<C> columns = row.getColumns();
							if (!columns.isEmpty()) {
								list.add(new CassandraRow<C>(
										CassandraConverter.toString(row.getKey(), keySerializer),
										CassandraConverter.toCassandraColumnMap(columns, columnSerializer, valueSerializer)
										));
							}
						}
					}
					return list;
				} else {
					// row as map if keys are array or single string
					if (gets.hasColumnRange()) {
						Map<String, List<CassandraColumn<C>>> maplist = new HashMap<>(rows.size());
						for (Row<K, C> row : rows) {
							String rowKey = CassandraConverter.toString(row.getKey(), keySerializer);
							ColumnList<C> columns = row.getColumns();
							if (!columns.isEmpty()) {
								maplist.put(rowKey, CassandraConverter.toCassandraColumnList(columns, columnSerializer));
							}
						}
						return maplist;
					} else {
						Map<String, Map<String, JsonNode>> mapmap = new HashMap<>(rows.size());
						for (Row<K, C> row : rows) {
							String rowKey = keySerializer.getString(keySerializer.toByteBuffer(row.getKey()));
							ColumnList<C> columns = row.getColumns();
							if (!columns.isEmpty()) {
								mapmap.put(rowKey, CassandraConverter.toCassandraColumnMap(columns, columnSerializer, valueSerializer));
							}
						}
						return mapmap;
					}
				}
			}
		} catch (ConnectionException e) {
			throw new TritonCassandraException(
					TritonErrors.cassandra_connection_fail,
					e);
		}
	}
	
	/**
	 * Create {@link ByteBufferRange} from {@link GetColumns} query.
	 * @param gets
	 * @param columnSerializer
	 * @return
	 */
	private ByteBufferRange createRange(JsonNode node, Serializer<?> columnSerializer) {
		
		ByteBuffer start = null;
		ByteBuffer end = null;
		Boolean reversed = Boolean.FALSE;
		Integer limit = DEFAULT_LIMIT_COLUMNS;
		
		if (node.has("start") && !node.get("start").isNull()) {
			// set start of the range
			start = getRangeBuffer(node.get("start"), columnSerializer, true);
		} else {
			start = EMPTY_BUFFER;
		}
		if (node.has("end") && !node.get("end").isNull()) {
			end = getRangeBuffer(node.get("end"), columnSerializer, false);
		} else {
			end = EMPTY_BUFFER;
		}
		// ignore if start or end node exists
		if (start == EMPTY_BUFFER && end == EMPTY_BUFFER) {
			if (node.has("startWith") && !node.get("startWith").isNull()) {
				String prefix = node.get("startWith").asText();
				start = getStartWithBuffer(prefix, columnSerializer, true);
				end = getStartWithBuffer(prefix, columnSerializer, false);
			}
		}
		
		if (node.has("reversed")) {
			// mark reversed
			reversed = node.get("reversed").asBoolean();
			// swap start/end
			ByteBuffer temp = start;
			start = end;
			end = temp;
		}
		if (node.has("limit")) {
			// set limit size of columns of single row
			limit = node.get("limit").asInt();
		}
		return new ByteBufferRangeImpl(start, end, limit, reversed);
	}
	
	/**
	 * Get buffer for the range.
	 * @param endpoint
	 * @param serializer
	 * @param start
	 * @return
	 */
	private <C> ByteBuffer getRangeBuffer(JsonNode endpoint, Serializer<C> serializer, boolean start) {
		if (endpoint.isObject()) {
			JsonNode value = endpoint.get("value");
			ByteBuffer point = serializer.fromString(value.asText());
			if (endpoint.has("exclusive") && endpoint.get("exclusive").asBoolean()) {
				if (start) {
					// get next binary
					point = serializer.getNext(point);
				} else {
					// get previous binary
					point = BytesUtil.previous(point);
				}
			}
			return point;
		} else {
			return serializer.fromString(endpoint.asText());
		}
	}
	
	/**
	 * Get buffer for the start with range.
	 * @param prefix
	 * @param serializer
	 * @param start
	 * @return
	 */
	private ByteBuffer getStartWithBuffer(String prefix,
			Serializer<?> columnSerializer, boolean start) {
		if (start) {
			return columnSerializer.fromString(prefix);
		} else {
			return columnSerializer.fromString(prefix+"\uffff");
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <C> String getRangeToken(
			JsonNode endpoint,
			Serializer<C> serializer,
			IPartitioner<? extends Token<?>> partitioner,
			boolean start) {
		
		// start token is default exclusive.
		// should get previous token if exclusive is not specified
		TokenFactory factory = partitioner.getTokenFactory();
		
		if (endpoint == null || endpoint.isNull()) {
			// return null if minimum/maximum token required
			return factory.toString(partitioner.getMinimumToken());
		} else if (endpoint.isObject()) {
			JsonNode value = endpoint.get("value");
			Token<?> token = null;
			if (value == null) {
				token = partitioner.getMinimumToken();
			} else {
				ByteBuffer point = serializer.fromString(value.asText());
				// get token string
				token = partitioner.getToken(point);
			}
			// get next/previous token if exlusive required
			if (endpoint.has("exclusive") && endpoint.get("exclusive").asBoolean()) {
				// get exclusive token
				if (start) {
					// start token is default to exclusive.
					return factory.toString(token);
				} else {
					// should get previous token
					return getPreviousToken(token);
				}
			} else {
				// get inclusive token
				if (start) {
					// start token is exlusive default
					// should get previous value to get inclusive value
					return getPreviousToken(token);
				} else {
					// end token is default to inclusive
					return factory.toString(token);
				}
			}
		} else {
			ByteBuffer point = serializer.fromString(endpoint.asText());
			// get token string
			Token<?> token = partitioner.getToken(point);
			// get inclusive token
			if (start) {
				// start token is exlusive default
				// should get previous value to get inclusive value
				return getPreviousToken(token);
			} else {
				// end token is default to inclusive
				return factory.toString(token);
			}
		}
	}
	
	/**
	 * Get next token
	 * @param object
	 */
	/*
	private <T> String getNextToken(Token<T> token) {
		T object = token.token;
		Class<?> objectClass = object.getClass();
		if (objectClass == BigInteger.class) {
			return ((BigInteger) object).add(BigInteger.ONE).toString();
		} else if (objectClass == Long.class) {
			return String.valueOf(((Long) object).longValue() + 1L);
		} else if (objectClass == byte[].class) {
			return Hex.bytesToHex(BytesUtil.next((byte[]) object));
		} else {
			throw new TritonCassandraException("Unsupported token type " + object.getClass().getSimpleName());
		}
	}
	*/
	
	/**
	 * Get previosu token
	 * @param object
	 */
	private <T> String getPreviousToken(Token<T> token) {
		T object = token.token;
		Class<?> objectClass = object.getClass();
		if (objectClass == BigInteger.class) {
			return ((BigInteger) object).subtract(BigInteger.ONE).toString();
		} else if (objectClass == Long.class) {
			return String.valueOf(((Long) object).longValue() - 1L);
		} else if (objectClass == byte[].class) {
			return Hex.bytesToHex(BytesUtil.previous((byte[]) object));
		} else {
			throw new TritonCassandraException(
					TritonErrors.cassandra_invalid_token_type,
					"Unsupported token type " + object.getClass().getSimpleName());
		}
	}
	
	@SuppressWarnings("unchecked")
	public <K,C> void setColumns(SetColumns sets) {
		
		// get keyspace
		Keyspace keyspace = getKeyspace(sets.getCluster(), sets.getKeyspace());
		// get column family
		ColumnFamily<K, C> cf = (ColumnFamily<K, C>) getColumnFamily(
				sets.getCluster(),
				sets.getKeyspace(),
				sets.getColumnFamily()
		);
		Integer ttl = sets.getTtl();
		// mutate
		MutationBatch batch = keyspace.prepareMutationBatch();
		// set consistency level if specified
		if (sets.getConsistency() != null) {
			batch.withConsistencyLevel(CassandraConverter.consistency(sets.getConsistency()));
		}
		for (Entry<String, Map<String, JsonNode>> row : sets.getRows().entrySet()) {
			String rowKey = row.getKey();
			Map<String, JsonNode> columns = row.getValue();
			// prepare column mutation
			ColumnListMutation<C> clm = batch.withRow(cf, CassandraConverter.toObject(rowKey, cf.getKeySerializer()));
			for (Entry<String, JsonNode> entry : columns.entrySet()) {
				String columnKey = entry.getKey();
				// convert string key to type
				C key = CassandraConverter.toObject(columnKey, cf.getColumnSerializer());
				// get buffer from serializer
				ByteBuffer value = CassandraConverter.toValueBuffer(entry.getValue(), cf.getDefaultValueSerializer());
				if (ttl == null) {
					// set column value
					clm.putColumn(key, value);
				} else {
					// set column with time to live
					clm.putColumn(key, value, ttl);
				}
			}
		}
		try {
			OperationResult<Void> result = batch.execute();
			result.getResult();
		} catch (ConnectionException e) {
			throw new TritonCassandraException(TritonErrors.cassandra_connection_fail, e);
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public <K,C> void removeColumns(RemoveColumns remove) {
		
		// get keyspace
		Keyspace keyspace = getKeyspace(remove.getCluster(), remove.getKeyspace());
		// get column family
		ColumnFamily<K,C> cf = (ColumnFamily<K, C>) getColumnFamily(
				remove.getCluster(),
				remove.getKeyspace(),
				remove.getColumnFamily());
		
		// prepare batch for deletion
		MutationBatch batch = keyspace.prepareMutationBatch();
		if (remove.getConsistency() != null) {
			// set consistency level if specified
			batch.withConsistencyLevel(
					CassandraConverter.consistency(remove.getConsistency())
			);
		}
		
		// iterate keys
		if (remove.hasKeys()) {
			for (String key : remove.getKeys()) {
				batch.withRow(cf, CassandraConverter.toObject(key, cf.getKeySerializer())).delete();
			}
		}
		
		// iterate rows
		if (remove.hasRows()) {
			for (Entry<String, List<String>> row : remove.getRows().entrySet()) {
				String rowKey = row.getKey();
				// get value list
				List<String> values = row.getValue();
				if (values != null && values.size() > 0) {
					// prepare row mutation
					ColumnListMutation<C> clm = batch.withRow(
							cf,
							CassandraConverter.toObject(rowKey, cf.getKeySerializer())
							);
					// remove all columns
					for (String column : values) {
						clm.deleteColumn(
								CassandraConverter.toObject(column, cf.getColumnSerializer())
								);
					}
				}
			}
		}
		try {
			batch.execute();
		} catch (ConnectionException e) {
			throw new TritonCassandraException(TritonErrors.cassandra_connection_fail, e);
		}
	}
	
	/**
	 * Close all resources for cassandra
	 */
	public void close() {
		log.info("shutting down cassandra contexts");
		for (ClusterHolder holder : clustermap.values()) {
			holder.shutdown();
		}
		for (KeyspaceHolder holder : keyspacemap.values()) {
			holder.shutdown();
		}
	}
	
	/**
	 * Clear column family cache
	 * @return
	 */
	public void clearColumnFamilyCache() {
		this.cfmap.clear();
	}
	
	@Override
	public void clean() {
		this.close();
	}
}
