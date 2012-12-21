package com.amebame.triton.service;

import com.amebame.triton.server.TritonMethod;

public class TritonHeartbeat {

	public TritonHeartbeat() {
	}

	@TritonMethod("triton.heartbeat")
	public void ping() {
	}
}
