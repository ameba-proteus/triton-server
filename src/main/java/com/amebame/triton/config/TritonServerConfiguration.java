package com.amebame.triton.config;


/**
 * Triton Configuration Properties
 */
public class TritonServerConfiguration {
	
	// Netty configuration
	private TritonNettyConfiguration netty = new TritonNettyConfiguration();
	
	// Cassandra
	private TritonCassandraConfiguration cassandra;
	
	// Memcached
	private TritonMemcachedConfiguration memcached;
	
	// Zookeeper
	private TritonZookeeperConfiguration zookeeper;
	
	public TritonServerConfiguration() {
	}

	public TritonNettyConfiguration getNetty() {
		return netty;
	}
	
	public void setNetty(TritonNettyConfiguration netty) {
		this.netty = netty;
	}
	
	public TritonCassandraConfiguration getCassandra() {
		return cassandra;
	}
	
	public void setCassandra(TritonCassandraConfiguration cassandra) {
		this.cassandra = cassandra;
	}
	
	public TritonMemcachedConfiguration getMemcached() {
		return memcached;
	}
	
	public void setMemcached(TritonMemcachedConfiguration memcached) {
		this.memcached = memcached;
	}
	
	public TritonZookeeperConfiguration getZookeeper() {
		return zookeeper;
	}
	
	public void setZookeeper(TritonZookeeperConfiguration zookeeper) {
		this.zookeeper = zookeeper;
	}
}
