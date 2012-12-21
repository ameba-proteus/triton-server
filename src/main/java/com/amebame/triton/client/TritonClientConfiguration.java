package com.amebame.triton.client;

import com.amebame.triton.config.TritonNettyConfiguration;

public class TritonClientConfiguration {
	
	private TritonNettyConfiguration netty;
	
	public TritonClientConfiguration() {
		netty = new TritonNettyConfiguration();
		netty.setBoss(1);
		netty.setWorker(4);
	}
	
	public TritonNettyConfiguration getNetty() {
		return netty;
	}
	
	public void setNetty(TritonNettyConfiguration netty) {
		this.netty = netty;
	}
	
}
