package com.amebame.triton.service.cassandra;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class CassandraRow<C> {
	
	private String key;
	
	private Object columns;
	
	public CassandraRow() {
	}

	public CassandraRow(String key, Object columns) {
		this.key = key;
		this.columns = columns;
	}
	
	public String getKey() {
		return key;
	}
	
	public Object getColumns() {
		return columns;
	}

}
