package com.amebame.triton.service.cassandra;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class CassandraRow {
	
	private String key;
	
	private List<CassandraColumn> columns;
	
	public CassandraRow(String key) {
		this(key, new ArrayList<CassandraColumn>());
	}

	public CassandraRow(String key, List<CassandraColumn> columns) {
		this.key = key;
		this.columns = columns;
	}
	
	public String getKey() {
		return key;
	}
	
	public List<CassandraColumn> getColumns() {
		return columns;
	}
	
	public void addColumn(CassandraColumn column) {
		this.columns.add(column);
	}

}
