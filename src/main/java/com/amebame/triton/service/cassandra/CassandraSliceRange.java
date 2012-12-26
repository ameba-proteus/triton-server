package com.amebame.triton.service.cassandra;

import java.util.List;

public class CassandraSliceRange<C> {
	
	private String start;
	
	private String end;
	
	private Boolean startExclusive;
	
	private List<String> columns;

	public CassandraSliceRange() {
	}

	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public void setEnd(String end) {
		this.end = end;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
	
	public void setStartExclusive(Boolean startExclusive) {
		this.startExclusive = startExclusive;
	}
	
	public Boolean getStartExclusive() {
		return startExclusive;
	}

}
