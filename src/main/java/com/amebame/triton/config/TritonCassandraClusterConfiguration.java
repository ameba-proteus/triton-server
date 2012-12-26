package com.amebame.triton.config;

import org.apache.commons.lang.StringUtils;

import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Configuration for the cassandra cluter
 */
public class TritonCassandraClusterConfiguration {
	
	private String name;
	
	private AstyanaxConfigurationImpl astyanaxConfig;
	
	private ConnectionPoolConfigurationImpl poolConfig;

	public TritonCassandraClusterConfiguration() {
		astyanaxConfig = new AstyanaxConfigurationImpl();
		// set default consistency to QUORUM
		astyanaxConfig.setDefaultReadConsistencyLevel(ConsistencyLevel.CL_QUORUM);
		astyanaxConfig.setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_QUORUM);
		poolConfig = new ConnectionPoolConfigurationImpl("astyanax-pool");
	}

	public AstyanaxConfigurationImpl getAstyanaxConfig() {
		return astyanaxConfig;
	}
	
	public ConnectionPoolConfigurationImpl getPoolConfig() {
		return poolConfig;
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
	
	/**
	 * Max connections per cassandra node
	 * @param maxConns
	 */
	public TritonCassandraClusterConfiguration setMaxConnsPerHost(int maxConns) {
		poolConfig.setMaxConnsPerHost(maxConns);
		return this;
	}
	
	/**
	 * Set seed hosts
	 * @param seeds
	 */
	public TritonCassandraClusterConfiguration setSeeds(String ... seeds) {
		poolConfig.setSeeds(StringUtils.join(seeds, ','));
		return this;
	}
	
	/**
	 * Set auto discovery
	 */
	public TritonCassandraClusterConfiguration setAutoDiscovery(boolean enable) {
		if (enable) {
			astyanaxConfig.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE);
		} else {
			astyanaxConfig.setDiscoveryType(NodeDiscoveryType.NONE);
		}
		return this;
	}
}
