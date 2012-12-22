package com.amebame.triton.service.cassandra.entity;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Create a keyspace
 */
@JsonInclude(Include.NON_NULL)
public class CreateKeyspace {
	
	// cluster name
	private String cluster;
	
	// keyspace name
	private String keyspace;
	
	// placement strategy
	@JsonProperty("strategy_class")
	private String strategyClass = "SimpleStrategy";
	
	// strategy options
	@JsonProperty("strategy_options")
	private Map<String, String> strategyOptions;
	
	@JsonProperty("durable_writes")
	private boolean durableWrites = true;
	
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
	
	public String getStrategyClass() {
		return strategyClass;
	}
	
	public void setStrategyClass(String strategyClass) {
		this.strategyClass = strategyClass;
	}
	
	public Map<String, String> getStrategyOptions() {
		return strategyOptions;
	}
	
	public void setStrategyOptions(Map<String, String> strategyOptions) {
		this.strategyOptions = strategyOptions;
	}
	
	public boolean isDurableWrites() {
		return durableWrites;
	}
	
	public void setDurableWrites(boolean durableWrites) {
		this.durableWrites = durableWrites;
	}
	
	@JsonIgnore
	public void setStrategyOption(String name, Object value) {
		if (strategyOptions == null) {
			strategyOptions = new HashMap<String, String>();
		}
		strategyOptions.put(name, String.valueOf(value));
	}
	
	@JsonIgnore
	public void setReplicationFactor(int replicationFactor) {
		setStrategyOption("replication_factor", replicationFactor);
	}

}
