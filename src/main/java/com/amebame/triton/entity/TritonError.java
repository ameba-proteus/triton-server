package com.amebame.triton.entity;

public class TritonError {
	
	private String message;
	
	public TritonError(Throwable e) {
		this.message = e.getMessage();
	}
	
	public TritonError(String message) {
		this.message = message;
	}
	
	public String getMessage() {
		return message;
	}

}
