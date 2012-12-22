package com.amebame.triton.service.cassandra.entity;


/**
 * Create a keyspace
 */
public class ListKeyspace {
	
	// cluster name
	private String cluster;
	
	public ListKeyspace() {
	}
	
	public String getCluster() {
		return cluster;
	}
	
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

}
