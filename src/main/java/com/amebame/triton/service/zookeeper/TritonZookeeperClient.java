package com.amebame.triton.service.zookeeper;

import javax.inject.Inject;

import org.apache.commons.lang.StringUtils;

import com.amebame.triton.config.TritonZookeeperConfiguration;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.retry.RetryOneTime;

public class TritonZookeeperClient {
	
	private CuratorFramework framework;
	
	@Inject
	public TritonZookeeperClient(TritonZookeeperConfiguration config) {
		
		String connectionString = StringUtils.join(config.getHosts(), ',');
		
		RetryPolicy retryPolicy = new RetryOneTime(100);
		if (config.getRetry() > 1) {
			retryPolicy = new ExponentialBackoffRetry(100, config.getRetry());
		}
		
		framework = CuratorFrameworkFactory.newClient(
				connectionString,
				retryPolicy
		);
		
	}
	
	public CuratorFramework getFramework() {
		return framework;
	}
	
}
