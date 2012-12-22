package com.amebame.triton.service.cassandra.entity;


/**
 * Get detail of the keyspace
 */
public class DescribeKeyspace {
	
	// cluster name
	private String cluster;
	
	// keyspace name
	private String keyspace;
	
	public DescribeKeyspace() {
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
	
}
