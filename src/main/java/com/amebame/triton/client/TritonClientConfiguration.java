package com.amebame.triton.client;

import com.amebame.triton.config.TritonNettyConfiguration;

public class TritonClientConfiguration {
	
	private TritonNettyConfiguration netty;
	
	private long commandTimeout = 5000L;
	
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
	
	public long getCommandTimeout() {
		return commandTimeout;
	}
	
	public void setCommandTimeout(long commandTimeout) {
		this.commandTimeout = commandTimeout;
	}
	
}
