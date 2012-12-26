package com.amebame.triton.service.cassandra;

import com.fasterxml.jackson.databind.JsonNode;

public class CassandraColumn<C> {
	
	private C column;
	private JsonNode value;

	public CassandraColumn() {
	}
	
	public CassandraColumn(C column, JsonNode value) {
		this.column = column;
		this.value = value;
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

}