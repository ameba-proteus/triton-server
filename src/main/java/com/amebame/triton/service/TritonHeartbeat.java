package com.amebame.triton.service;

import com.amebame.triton.server.TritonMethod;

public class TritonHeartbeat {

	public TritonHeartbeat() {
	}

	@TritonMethod("triton.heartbeat")
	public long ping() {
		return (int) (System.currentTimeMillis() / 1000L);
	}
	
	@TritonMethod("triton.echo")
	public String echo(String text) {
		return text;
	}
}
