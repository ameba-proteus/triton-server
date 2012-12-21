package com.amebame.triton.entity;

import com.fasterxml.jackson.databind.JsonNode;

public class TritonBody {
	
	private String name;
	private JsonNode body;

	public TritonBody() {
	}
	
	public TritonBody(String name, JsonNode body) {
		this.name = name;
		this.body = body;
	}
	
	public String getName() {
		return name;
	}
	
	public JsonNode getBody() {
		return body;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setBody(JsonNode body) {
		this.body = body;
	}

}
