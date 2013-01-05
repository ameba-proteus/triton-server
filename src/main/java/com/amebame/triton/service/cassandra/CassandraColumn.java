package com.amebame.triton.service.cassandra;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;

@JsonInclude(Include.NON_NULL)
public class CassandraColumn<C> {
	
	private C column;
	private JsonNode value;
	private long timestamp;

	public CassandraColumn() {
	}
	
	public CassandraColumn(C column, JsonNode value, long timestamp) {
		this.column = column;
		this.value = value;
		this.timestamp = timestamp;
	}

	public C getColumn() {
		return column;
	}

	public void setColumn(C column) {
		this.column = column;
	}

	public JsonNode getValue() {
		return value;
	}

	public void setValue(JsonNode value) {
		this.value = value;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

}
