package com.amebame.triton.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.amebame.triton.server.TritonCleaner;

/**
 * ElasticSearch configurations
 */
public class TritonElasticSearchConfiguration implements TritonCleaner {

	// Cluster configuraiton map
	private Map<String, TritonElasticSearchClusterConfiguration> clusters;
	
	// Timeout for ElasticSearch (default 5 sec)
	private long timeout = 5000L;
	
	public TritonElasticSearchConfiguration() {
		clusters = new HashMap<>();
	}
	
	@Override
	public void clean() {
	}

	/**
	 * Set cluster configuraiton map
	 * @param clusters
	 */
	public void setClusters(Map<String, TritonElasticSearchClusterConfiguration> clusters) {
		this.clusters = clusters;
	}
	
	/**
	 * Get cluster configuraiton map
	 * @return
	 */
	public Map<String, TritonElasticSearchClusterConfiguration> getClusters() {
		return clusters;
	}
	
	/**
	 * Get cluster keys which are registered
	 * @return
	 */
	public Set<String> getClusterKeys() {
		return clusters.keySet();
	}

	/**
	 * Get cluster configuration.
	 * @param key
	 * @return
	 */
	public TritonElasticSearchClusterConfiguration getClusterConfiguration(String key) {
		return clusters.get(key);
	}
	
	/**
	 * Set the timeout value in milliseconds.
	 * @param timeout
	 */
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}
	
	/**
	 * Get the milliseconds of timeout.
	 * @return
	 */
	public long getTimeout() {
		return timeout;
	}
}
