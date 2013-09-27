package com.amebame.triton.config;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

/**
 * Configuration for the cassandra cluter
 */
public class TritonCassandraClusterConfiguration {
	
	private String name;
	
	private int port;
	
	private String[] seeds;
	
	private TritonCassandraClusterCredential credential;
	
	private Compression compression;
	
	private ConsistencyLevel consistencyLevel;
	
	private ConsistencyLevel serialConsistencyLevel;
	
	private RetryPolicy retryPolicy;
	
	private ReconnectionPolicy reconnectionPolicy;
	
	private LoadBalancingPolicy loadBalancingPolicy;
	
	private PoolingOptions poolingOptions;
	
	public TritonCassandraClusterConfiguration() {
		consistencyLevel = ConsistencyLevel.QUORUM;
		serialConsistencyLevel = ConsistencyLevel.SERIAL;
		port = 9042;
		compression = Compression.NONE;
		retryPolicy = DefaultRetryPolicy.INSTANCE;
		reconnectionPolicy = new ConstantReconnectionPolicy(1000L);
		loadBalancingPolicy = new RoundRobinPolicy();
		poolingOptions = new PoolingOptions();
	}
	
	/**
	 * Cluster name
	 * @return
	 */
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public boolean hasCredential() {
		return credential != null;
	}
	
	public TritonCassandraClusterCredential getCredential() {
		return credential;
	}
	
	public void setCredential(TritonCassandraClusterCredential credential) {
		this.credential = credential;
	}
	
	/**
	 * Get the default consistency level.
	 * @return
	 */
	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}
	
	/**
	 * Set the default consistency level.
	 * @param consistencyLevel
	 */
	public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}
	
	/**
	 * Get the default serial consistency level.
	 * @return
	 */
	public ConsistencyLevel getSerialConsistencyLevel() {
		return serialConsistencyLevel;
	}
	
	/**
	 * Set the default serial consistency level.
	 * @param reconnectionPolicy
	 */
	public void setReconnectionPolicy(ReconnectionPolicy reconnectionPolicy) {
		this.reconnectionPolicy = reconnectionPolicy;
	}
	
	/**
	 * Set the load balancing policy
	 * @param clazz
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public void setLoadBalancingPolicy(Class<? extends LoadBalancingPolicy> clazz) throws InstantiationException, IllegalAccessException {
		loadBalancingPolicy = clazz.newInstance();
	}
	
	/**
	 * Set the constant reconnection policy.
	 * @param delay
	 */
	public void setConstantReconnection(long delay) {
		reconnectionPolicy = new ConstantReconnectionPolicy(delay);
	}
	
	/**
	 * Get the pooling options.
	 * @return
	 */
	public PoolingOptions getPoolingOptions() {
		return poolingOptions;
	}
	
	/**
	 * Set the pooling options.
	 * @param poolingOptions
	 */
	public void setPoolingOptions(PoolingOptions poolingOptions) {
		this.poolingOptions = poolingOptions;
	}
	
	/**
	 * Set seed hosts
	 * @param seeds
	 */
	public TritonCassandraClusterConfiguration setSeeds(String ... seeds) {
		this.seeds = seeds;
		return this;
	}
	
	/**
	 * Get seed hosts
	 * @return
	 */
	public String[] getSeeds() {
		return seeds;
	}
	
	/**
	 * Set the port.
	 * @param port
	 */
	public void setPort(int port) {
		this.port = port;
	}

	public int getPort() {
		return port;
	}
	
	/**
	 * Set token aware pool type
	 * @param enable
	 * @return
	 */
	public TritonCassandraClusterConfiguration setTokenAware(boolean enable) {
		if (enable) {
			loadBalancingPolicy = new TokenAwarePolicy(new RoundRobinPolicy());
		} else {
			loadBalancingPolicy = new RoundRobinPolicy();
		}
		return this;
	}
	
	/**
	 * Set the compression.
	 * 'none' or 'snappy'
	 * @param compression
	 */
	public void setCompression(String compression) {
		this.compression = Compression.valueOf(compression.toUpperCase());
	}
	
	public Compression getCompression() {
		return compression;
	}
	
	/**
	 * Get the load balancing policy.
	 * @return
	 */
	public LoadBalancingPolicy getLoadBalancingPolicy() {
		return loadBalancingPolicy;
	}
	
	/**
	 * Get the reconnection policy.
	 * @return
	 */
	public ReconnectionPolicy getReconnectionPolicy() {
		return reconnectionPolicy;
	}
	
	/**
	 * Get the retry policy.
	 * @return
	 */
	public RetryPolicy getRetryPolicy() {
		return retryPolicy;
	}
	
	/**
	 * Triton cassandra cluster credential
	 * @author namura_suguru
	 */
	public static class TritonCassandraClusterCredential {
		private String user;
		private String password;
		public String getPassword() {
			return password;
		}
		public String getUser() {
			return user;
		}
		public void setPassword(String password) {
			this.password = password;
		}
		public void setUser(String user) {
			this.user = user;
		}
	}
}
