package com.amebame.triton.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Cassandra configuration properties for triton
 */
public class TritonCassandraConfiguration {
	
	// Cluster Map
	private Map<String, TritonCassandraClusterConfiguration> clusters;
	
	public TritonCassandraConfiguration() {
		this.clusters = new HashMap<String, TritonCassandraClusterConfiguration>();
	}
	
	/**
	 * Set cluster configurations
	 * @param clusters
	 * @return
	 */
	public TritonCassandraConfiguration setClusters(Map<String, TritonCassandraClusterConfiguration> clusters) {
		this.clusters = clusters;
		return this;
	}
	
	/**
	 * Get cluster configurations
	 * @return
	 */
	public Map<String, TritonCassandraClusterConfiguration> getClusters() {
		return clusters;
	}
	
	/**
	 * Get cluster configuration 
	 * @param clusterName
	 * @return
	 */
	public TritonCassandraClusterConfiguration getCluster(String clusterName) {
		return clusters.get(clusterName);
	}
	
	/**
	 * Set cluster configuration
	 * @param clusterName
	 * @param config
	 * @return
	 */
	public TritonCassandraConfiguration setCluster(String clusterName, TritonCassandraClusterConfiguration config) {
		this.clusters.put(clusterName, config);
		return this;
	}
	

}
