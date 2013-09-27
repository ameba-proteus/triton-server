package com.amebame.triton.service.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Inject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.amebame.triton.config.TritonCassandraClusterConfiguration;
import com.amebame.triton.config.TritonCassandraConfiguration;
import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.server.TritonCleaner;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
/**
 * Client for cassandra clusters
 */
public class TritonCassandraClient implements TritonCleaner {
	
	private static final Logger log = LogManager.getLogger(TritonCassandraClient.class);
	
	private TritonCassandraConfiguration config;
	
	private Map<String, Cluster> clusters;
	
	private Map<String, ConcurrentMap<String, Session>> sessions;
	
	private ConcurrentMap<String, Session> clusterSessions;
	
	@Inject
	public TritonCassandraClient(TritonCassandraConfiguration config) {
		this.config = config;
		log.info("initialized triton cassandra client");
		this.clusters = new HashMap<>();
		this.sessions = new HashMap<>();
		for (String key : config.getClusters().keySet()) {
			clusters.put(key, createCluster(key));
			sessions.put(key, new ConcurrentHashMap<String, Session>());
		}
		this.clusterSessions = new ConcurrentHashMap<>();
	}
	
	/**
	 * Create the clsuter client.
	 * @param clusterName
	 * @return
	 */
	private Cluster createCluster(String clusterName) {
		log.info("creating cluster client of {}", clusterName);
		TritonCassandraClusterConfiguration clusterConfig = getClusterConfig(clusterName);
		// build the astyanax context
		Cluster.Builder builder = Cluster.builder()
				.addContactPoints(clusterConfig.getSeeds())
				.withCompression(clusterConfig.getCompression())
				.withLoadBalancingPolicy(clusterConfig.getLoadBalancingPolicy())
				.withPort(clusterConfig.getPort())
				.withReconnectionPolicy(clusterConfig.getReconnectionPolicy())
				.withRetryPolicy(clusterConfig.getRetryPolicy())
				.withQueryOptions(
						new QueryOptions()
						.setConsistencyLevel(clusterConfig.getConsistencyLevel())
						.setSerialConsistencyLevel(clusterConfig.getSerialConsistencyLevel())
						.setFetchSize(Integer.MAX_VALUE) // TODO workaround to prevent bugs in cassandra 2.0.0 pager
				)
				;
		if (clusterConfig.hasCredential()) {
			builder.withCredentials(
					clusterConfig.getCredential().getUser(),
					clusterConfig.getCredential().getPassword()
					);
		}
		return builder.build();
	}
	
	/**
	 * Get cluster holder
	 * @param clusterName
	 * @return
	 */
	public Cluster getCluster(String clusterName) {
		Cluster cluster = clusters.get(clusterName);
		if (cluster == null) {
			throw new TritonCassandraException(TritonErrors.cassandra_no_cluster, "cluster " + clusterName + " is not configured");
		}
		return cluster;
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
	 * Get global {@link Session} to control cluster session
	 * @return
	 */
	public Session getSession(String clusterName) {
		Cluster cluster = getCluster(clusterName);
		Session session = clusterSessions.get(clusterName);
		if (session == null) {
			Session newSession = cluster.connect();
			session = clusterSessions.putIfAbsent(clusterName, newSession);
			if (session != null) {
				// shutdown unused session
				newSession.shutdown();
				return session;
			} else {
				return newSession;
			}
		} else {
			return session;
		}
	}
	
	/**
	 * Get {@link Session} of the keyspace.
	 * It holds single {@link Session} instance per keyspace.
	 * @param clusterName
	 * @param keyspaceName
	 * @return
	 */
	public Session getSession(String clusterName, String keyspaceName) {
		// get keyspace to check existance
		Cluster cluster = getCluster(clusterName);
		ConcurrentMap<String, Session> map = sessions.get(clusterName);
		Session session = map.get(keyspaceName);
		if (session == null) {
			Session newSession = cluster.connect(keyspaceName);
			session = map.putIfAbsent(keyspaceName, newSession);
			if (session != null) {
				// shutdown unused session
				newSession.shutdown();
				return session;
			} else {
				return newSession;
			}
		} else {
			return session;
		}
	}
	
	/**
	 * Get keyspace list
	 * @param cluster
	 * @return
	 */
	public List<KeyspaceMetadata> getKeyspaces(String clusterName) {
		Cluster cluster = getCluster(clusterName);
		return cluster.getMetadata().getKeyspaces();
	}
	
	/**
	 * Get keyspace definition
	 * @param clusterName
	 * @param keyspaceName
	 * @return
	 */
	public KeyspaceMetadata getKeyspace(String clusterName, String keyspaceName) {
		Cluster cluster = getCluster(clusterName);
		KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(keyspaceName);
		if (keyspace == null) {
			throw new TritonCassandraException(TritonErrors.cassandra_no_keyspace, "keyspace [" + keyspaceName + "] does not exist");
		}
		return keyspace;
	}
	
	/**
	 * Get table list
	 * @param clusterName
	 * @param keyspaceName
	 * @return
	 */
	public List<TableMetadata> getTables(String clusterName, String keyspaceName) {
		return new ArrayList<>(getKeyspace(clusterName, keyspaceName).getTables());
	}
	
	/**
	 * Get table definition
	 * @param clusterName
	 * @param keyspaceName
	 * @param tableName
	 * @return
	 */
	public TableMetadata getTable(String clusterName, String keyspaceName, String tableName) {
		KeyspaceMetadata keyspace = getKeyspace(clusterName, keyspaceName);
		TableMetadata table = keyspace.getTable(tableName);
		if (table == null) {
			throw new TritonCassandraException(TritonErrors.cassandra_no_column_family, "table [" + tableName + "] does not exist");
		}
		return table;
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
	 * Close all resources for cassandra
	 */
	public void close() {
		log.info("shutting down cassandra clusters");
		for (Cluster cluster : clusters.values()) {
			cluster.shutdown();
		}
	}
	
	@Override
	public void clean() {
		this.close();
	}
}
