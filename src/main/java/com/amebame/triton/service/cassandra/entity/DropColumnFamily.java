package com.amebame.triton.service.cassandra.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DropColumnFamily {
	
	private String cluster;
	
	private String keyspace;
	
	@JsonProperty("column_family")
	private String columnFamily;

	public DropColumnFamily() {
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

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

}
