package com.amebame.triton.service.cassandra.entity;

public class DropKeyspace {
	
	private String cluster;
	
	private String keyspace;

	public DropKeyspace() {
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
