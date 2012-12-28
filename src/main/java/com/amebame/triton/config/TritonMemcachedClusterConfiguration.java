package com.amebame.triton.config;

public class TritonMemcachedClusterConfiguration {
	
	private String[] hosts;
	
	private TritonMemcachedLocator locator = TritonMemcachedLocator.simple;

	public TritonMemcachedClusterConfiguration() {
	}

	public String[] getHosts() {
		return hosts;
	}
	
	public void setHosts(String[] hosts) {
		this.hosts = hosts;
	}
	
	public void setHost(String host) {
		this.hosts = new String[] {host};
	}
	
	public TritonMemcachedLocator getLocator() {
		return locator;
	}
	
	public void setLocator(TritonMemcachedLocator locator) {
		this.locator = locator;
	}
}
