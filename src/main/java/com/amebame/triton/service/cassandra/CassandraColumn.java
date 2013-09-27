package com.amebame.triton.service.cassandra;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;

@JsonInclude(Include.NON_NULL)
public class CassandraColumn {
	
	private Object column;
	private JsonNode value;

	public CassandraColumn() {
	}
	
	public CassandraColumn(Object column, JsonNode value) {
		this.column = column;
		this.value = value;
	}

	public Object getColumn() {
		return column;
	}

	public void setColumn(Object column) {
		this.column = column;
	}

	public JsonNode getValue() {
		return value;
	}

	public void setValue(JsonNode value) {
		this.value = value;
	}
	
}
