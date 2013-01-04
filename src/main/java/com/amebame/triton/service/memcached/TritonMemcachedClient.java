package com.amebame.triton.service.memcached;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import net.rubyeye.xmemcached.HashAlgorithm;
import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.BinaryCommandFactory;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.impl.KetamaMemcachedSessionLocator;
import net.rubyeye.xmemcached.utils.AddrUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amebame.triton.config.TritonMemcachedClusterConfiguration;
import com.amebame.triton.config.TritonMemcachedConfiguration;
import com.amebame.triton.config.TritonMemcachedLocator;
import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.server.TritonCleaner;
import com.fasterxml.jackson.databind.JsonNode;

public class TritonMemcachedClient implements TritonCleaner {
	
	private static final Logger log = LogManager.getLogger(TritonMemcachedClient.class);
	
	private TritonMemcachedConfiguration config;
	
	private Map<String, MemcachedClient> clients;
	
	private MemcachedJsonTranscoder transcoder = new MemcachedJsonTranscoder();
	
	@Inject
	public TritonMemcachedClient(TritonMemcachedConfiguration config) throws IOException {
		this.config = config;
		this.clients = new HashMap<>();
		// creating cluster configuration
		for (Entry<String, TritonMemcachedClusterConfiguration> entry : config.getClusters().entrySet()) {
			String clusterKey = entry.getKey();
			TritonMemcachedClusterConfiguration  clusterConfig = entry.getValue();
			MemcachedClient client = createClient(clusterKey, clusterConfig);
			clients.put(clusterKey, client);
		}
	}
	
	@Override
	public void clean() {
		// nothing to clear
	}
	
	/**
	 * Get configuration instance.
	 * @return
	 */
	public TritonMemcachedConfiguration getConfiguration() {
		return config;
	}
	
	/**
	 * Get {@link MemcachedClient} from cluster.
	 * @param cluster
	 * @return
	 */
	public MemcachedClient getClient(String cluster) {
		MemcachedClient client = clients.get(cluster);
		if (client == null) {
			throw new TritonMemcachedException(
					TritonErrors.memcached_not_cofigured,
					"memcached cluster " + cluster + " is not configured");
		}
		return client;
	}
	
	/**
	 * Store value to cache
	 * @param cluster
	 * @param key cache key
	 * @param seconds expire time in seconds
	 * @param value storing value
	 */
	public void set(String cluster, String key, int seconds, JsonNode value) {
		MemcachedClient client = getClient(cluster);
		try {
			client.set(key, seconds, value, transcoder);
		} catch (MemcachedException | InterruptedException e) {
			throw new TritonMemcachedException(
					TritonErrors.memcached_error,
					e);
		} catch (TimeoutException e) {
			throw new TritonMemcachedException(
					TritonErrors.memcached_timeout,
					e);
		}
	}
	
	/**
	 * Get value from the cache. Return null if not exists.
	 * if newSeconds has positive value, getAndTouch will be
	 * called and extend cache expiration.
	 * @param cluster
	 * @param key
	 * @param newSeconds
	 * @return
	 */
	public JsonNode get(String cluster, String key, Integer newSeconds) {
		MemcachedClient client = getClient(cluster);
		try {
			if (newSeconds == null) {
				return client.get(key, transcoder);
			} else {
				return client.getAndTouch(key, newSeconds);
			}
		} catch (MemcachedException | InterruptedException e) {
			throw new TritonMemcachedException(
					TritonErrors.memcached_error,
					e);
		} catch (TimeoutException e) {
			throw new TritonMemcachedException(
					TritonErrors.memcached_timeout,
					e);
		}
	}
	
	/**
	 * Get multiple values by list of string.
	 * @param cluster
	 * @param keys
	 * @return
	 */
	public Map<String, JsonNode> getMulti(String cluster, List<String> keys) {
		MemcachedClient client = getClient(cluster);
		try {
			return client.get(keys, transcoder);
		} catch (MemcachedException | InterruptedException e) {
			throw new TritonMemcachedException(
					TritonErrors.memcached_error,
					e);
		} catch (TimeoutException e) {
			throw new TritonMemcachedException(
					TritonErrors.memcached_timeout,
					e);
		}
	}
	
	/**
	 * Delete key from cache
	 * @param cluster
	 * @param key
	 */
	public void delete(String cluster, String key) {
		MemcachedClient client = getClient(cluster);
		try {
			client.delete(key);
		} catch (MemcachedException | InterruptedException e) {
			throw new TritonMemcachedException(
					TritonErrors.memcached_error,
					e);
		} catch (TimeoutException e) {
			throw new TritonMemcachedException(
					TritonErrors.memcached_timeout,
					e);
		}
	}
	
	/**
	 * Create client from {@link TritonMemcachedClusterConfiguration}
	 * @param config
	 * @return
	 */
	private MemcachedClient createClient(String name, TritonMemcachedClusterConfiguration config) throws IOException {
		
		String hosts = StringUtils.join(config.getHosts(), ' ');
		log.info("creating memcached client with ", hosts);
		
		XMemcachedClientBuilder builder = new XMemcachedClientBuilder(
				AddrUtil.getAddresses(hosts)
		);
		if (config.getLocator() == TritonMemcachedLocator.consistent_hash) {
			builder.setSessionLocator(new KetamaMemcachedSessionLocator(HashAlgorithm.FNV1_64_HASH));
		}
		builder.setName(name);
		builder.setCommandFactory(new BinaryCommandFactory());
		return builder.build();
		
	}

}