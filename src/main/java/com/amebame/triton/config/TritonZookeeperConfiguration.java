package com.amebame.triton.config;

import java.util.List;

public class TritonZookeeperConfiguration {
	
	private List<String> hosts;
	
	private int retry = 2;

	public TritonZookeeperConfiguration() {
	}
	
	public void setHosts(List<String> hosts) {
		this.hosts = hosts;
	}
	
	public List<String> getHosts() {
		return hosts;
	}
	
	public void setRetry(int retry) {
		this.retry = retry;
	}
	
	public int getRetry() {
		return retry;
	}

}
