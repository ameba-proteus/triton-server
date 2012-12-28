package com.amebame.triton.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Memcached configuration
 */
public class TritonMemcachedConfiguration {
	
	private Map<String, TritonMemcachedClusterConfiguration> clusters;

	public TritonMemcachedConfiguration() {
		clusters = new HashMap<>();
	}
	
	/**
	 * Get cluster map
	 * @return
	 */
	public Map<String, TritonMemcachedClusterConfiguration> getClusters() {
		return clusters;
	}
	
	/**
	 * Set cluster map
	 * @param clusters
	 */
	public void setClusters(Map<String, TritonMemcachedClusterConfiguration> clusters) {
		this.clusters = clusters;
	}

}
