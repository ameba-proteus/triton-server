package com.amebame.triton.service.cassandra.entity;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Create a keyspace
 */
public class CreateKeyspace {
	
	// cluster name
	private String cluster;
	
	// keyspace name
	private String keyspace;
	
	// placement strategy
	private String placementStrategy = "org.apache.cassandra.locator.SimpleStrategy";
	
	// strategy options
	private Map<String, Object> strategyOptions;

	public CreateKeyspace() {
	}
	
	public String getCluster() {
		return cluster;
	}
	
	public String getKeyspace() {
		return keyspace;
	}
	
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
	
	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}
	
	@JsonProperty("placement_strategy")
	public String getPlacementStrategy() {
		return placementStrategy;
	}
	
	@JsonProperty("placement_strategy")
	public void setPlacementStrategy(String placementStrategy) {
		this.placementStrategy = placementStrategy;
	}
	
	@JsonProperty("strategy_options")
	public Map<String, Object> getStrategyOptions() {
		return strategyOptions;
	}
	
	@JsonProperty("strategy_options")
	public void setStrategyOptions(Map<String, Object> strategyOptions) {
		this.strategyOptions = strategyOptions;
	}

}
