package com.amebame.triton.service.cassandra;

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
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class TritonCassandraClient implements TritonCleaner {
	
	private static final Logger log = LogManager.getLogger(TritonCassandraClient.class);
	
	private ConcurrentMap<String, AstyanaxContext<Keyspace>> keyspacemap;
	
	private TritonCassandraConfiguration config;
	
	private Lock lock;

	@Inject
	public TritonCassandraClient(TritonCassandraConfiguration config) {
		this.keyspacemap = new ConcurrentHashMap<String, AstyanaxContext<Keyspace>>();
		this.lock = new ReentrantLock();
		this.config = config;
		log.info("initialized triton cassandra client");
	}
	
	/**
	 * Get keyspace instance
	 * @param clusterName
	 * @param keyspaceName
	 * @return
	 */
	public Keyspace getKeyspace(String clusterName, String keyspaceName) {
		String key = getKey(clusterName, keyspaceName);
		AstyanaxContext<Keyspace> context = keyspacemap.get(key);
		if (context == null) {
			lock.lock();
			try {
				context = keyspacemap.get(key);
				if (context == null) {
					TritonCassandraClusterConfiguration clusterConfig = config.getCluster(clusterName);
					// return null if cluster is not defined
					if (clusterName == null) {
						log.warn("Cluster has not been configured for {}", clusterName);
						return null;
					}
					// build the astyanax context
					context = new AstyanaxContext.Builder()
					.forCluster(clusterName)
					.forKeyspace(keyspaceName)
					.withAstyanaxConfiguration(clusterConfig.getAstyanaxConfig())
					.withConnectionPoolConfiguration(clusterConfig.getPoolConfig())
					.buildKeyspace(ThriftFamilyFactory.getInstance());
					keyspacemap.put(key, context);
				}
			} finally {
				lock.unlock();
			}
		}
		Keyspace keyspace = context.getEntity();
		return keyspace;
	}

	/**
	 * Construct hash key for the keyspace
	 * @param cluster
	 * @param keyspace
	 * @return
	 */
	private String getKey(String cluster, String keyspace) {
		return cluster + '.' + keyspace;
	}
	
	/**
	 * Close all resources for cassandra
	 */
	public void close() {
		for (AstyanaxContext<Keyspace> context : keyspacemap.values()) {
			context.shutdown();
		}
	}
	
	@Override
	public void clean() {
		this.close();
	}
}
