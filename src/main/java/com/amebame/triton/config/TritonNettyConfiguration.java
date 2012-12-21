package com.amebame.triton.config;

public class TritonNettyConfiguration {
	
	// Maximum boss thread
	private int boss = 2;
	
	// Maximum worker thread
	private int worker = 10;
	
	// Connect timeout for client (default 5sec)
	private int connectTimeout = 5000;

	public TritonNettyConfiguration() {
	}

	public int getBoss() {
		return boss;
	}

	public void setBoss(int boss) {
		this.boss = boss;
	}

	public int getWorker() {
		return worker;
	}

	public void setWorker(int worker) {
		this.worker = worker;
	}
	
	public int getConnectTimeout() {
		return connectTimeout;
	}
	
	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}
}
