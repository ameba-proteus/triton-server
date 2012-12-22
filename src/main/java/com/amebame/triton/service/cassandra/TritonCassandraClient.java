package com.amebame.triton.service.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.inject.Inject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.amebame.triton.config.TritonCassandraClusterConfiguration;
import com.amebame.triton.config.TritonCassandraConfiguration;
import com.amebame.triton.server.TritonCleaner;
import com.amebame.triton.service.cassandra.entity.TritonColumnFamily;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.UnknownComparatorException;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Client for cassandra clusters
 */
public class TritonCassandraClient implements TritonCleaner {
	
	private static final Logger log = LogManager.getLogger(TritonCassandraClient.class);
	
	private ConcurrentMap<String, AstyanaxContext<Cluster>> clustermap;
	
	private ConcurrentMap<String, AstyanaxContext<Keyspace>> keyspacemap;
	
	private ConcurrentMap<String, TritonColumnFamily> cfmap;
	
	private TritonCassandraConfiguration config;
	
	private Lock lock;

	@Inject
	public TritonCassandraClient(TritonCassandraConfiguration config) {
		this.clustermap = new ConcurrentHashMap<String, AstyanaxContext<Cluster>>();
		this.keyspacemap = new ConcurrentHashMap<String, AstyanaxContext<Keyspace>>();
		this.cfmap = new ConcurrentHashMap<String, TritonColumnFamily>();
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
	 * Get keyspace list
	 * @param cluster
	 * @return
	 */
	public List<KeyspaceDefinition> getKeyspaceDefinitions(String clusterName) {
		AstyanaxContext<Cluster> context = getClusterContext(clusterName);
		try {
			Cluster cluster = context.getEntity();
			return cluster.describeKeyspaces();
		} catch (ConnectionException e) {
			throw new TritonCassandraException(e);
		}
	}
	
	/**
	 * Get keyspace definition
	 * @param clusterName
	 * @param keyspaceName
	 * @return
	 */
	public KeyspaceDefinition getKeyspaceDefinition(String clusterName, String keyspaceName) {
		AstyanaxContext<Keyspace> context = getKeyspaceContext(clusterName, keyspaceName);
		try {
			return context.getEntity().describeKeyspace();
		} catch (ConnectionException e) {
			throw new TritonCassandraException(e);
		}
	}
	
	/**
	 * Get ring list of the keyspace
	 * @param clusterName
	 * @param keyspaceName
	 * @return
	 */
	public List<TokenRange> getTokenRange(String clusterName, String keyspaceName) {
		AstyanaxContext<Keyspace> context = getKeyspaceContext(clusterName, keyspaceName);
		try {
			return context.getEntity().describeRing();
		} catch (ConnectionException e) {
			throw new TritonCassandraException(e);
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
	 * Get cluster context
	 * @param clusterName
	 * @return
	 */
	private AstyanaxContext<Cluster> getClusterContext(String clusterName) {
		AstyanaxContext<Cluster> context = clustermap.get(clusterName);
		if (context == null) {
			lock.lock();
			try {
				context = clustermap.get(clusterName);
				if (context == null) {
					log.info("creating cassandra cluter context for {}", clusterName);
					TritonCassandraClusterConfiguration clusterConfig = getClusterConfig(clusterName);
					// build the astyanax context
					context = new AstyanaxContext.Builder()
					.forCluster(clusterName)
					.withAstyanaxConfiguration(clusterConfig.getAstyanaxConfig())
					.withConnectionPoolConfiguration(
							new ConnectionPoolConfigurationImpl(clusterName+"-pool").setMaxConnsPerHost(1)
					)
					.buildCluster(ThriftFamilyFactory.getInstance());
					context.start();
					clustermap.put(clusterName, context);
				}
			} finally {
				lock.unlock();
			}
		}
		return context;
	}
	
	/**
	 * Get keyspace context
	 * @param clusterName
	 * @param keyspaceName
	 * @return
	 */
	private AstyanaxContext<Keyspace> getKeyspaceContext(String clusterName, String keyspaceName) {
		String key = getKey(clusterName, keyspaceName);
		AstyanaxContext<Keyspace> context = keyspacemap.get(key);
		if (context == null) {
			lock.lock();
			try {
				context = keyspacemap.get(key);
				if (context == null) {
					log.info("creating keyspace context for {} in {}", keyspaceName, clusterName);
					TritonCassandraClusterConfiguration clusterConfig = getClusterConfig(clusterName);
					// build the astyanax context
					context = new AstyanaxContext.Builder()
					.forCluster(clusterName)
					.forKeyspace(keyspaceName)
					.withAstyanaxConfiguration(clusterConfig.getAstyanaxConfig())
					.withConnectionPoolConfiguration(clusterConfig.getPoolConfig())
					.buildKeyspace(ThriftFamilyFactory.getInstance());
					context.start();
					keyspacemap.put(key, context);
				}
			} finally {
				lock.unlock();
			}
		}
		return context;
	}
	
	/**
	 * Get {@link TritonColumnFamily}. It will update keyspace definition if not exists.
	 * @param clusterName
	 * @param keyspaceName
	 * @param columnFamilyName
	 * @return
	 */
	public TritonColumnFamily getColumnFamily(String clusterName, String keyspaceName, String columnFamilyName) {
		String key = getKey(clusterName, keyspaceName, columnFamilyName);
		TritonColumnFamily cf = cfmap.get(key);
		if (cf == null) {
			updateKeyspaceDefinition(clusterName, keyspaceName);
			cf = cfmap.get(key);
			if (cf == null) {
				throw new TritonCassandraException("column family does not exists for " + columnFamilyName + " in " + keyspaceName);
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
						sp.getColumnNameSerializer());
				cfmap.put(key, new TritonColumnFamily(cfdef, cf, sp));
			}
		} catch (ConnectionException | UnknownComparatorException e) {
			throw new TritonCassandraException(e);
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
			throw new TritonCassandraException("cluster has not been configured for " + clusterName);
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
	 * Close all resources for cassandra
	 */
	public void close() {
		log.info("shutting down cassandra contexts");
		for (AstyanaxContext<Cluster> context : clustermap.values()) {
			context.shutdown();
		}
		for (AstyanaxContext<Keyspace> context : keyspacemap.values()) {
			context.shutdown();
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
