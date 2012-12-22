package com.amebame.triton.service.cassandra.entity;


/**
 * List column families
 */
public class ListColumnFamily {
	
	// cluster name
	private String cluster;
	
	// keyspace name
	private String keyspace;
	
	public ListColumnFamily() {
	}
	
	public String getCluster() {
		return cluster;
	}
	
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
	
	public String getKeyspace() {
		return keyspace;
	}
	
	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

}
