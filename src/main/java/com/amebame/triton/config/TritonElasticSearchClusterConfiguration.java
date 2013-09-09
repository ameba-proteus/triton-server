package com.amebame.triton.config;

import net.rubyeye.xmemcached.utils.AddrUtil;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Configuration of elastic search cluster.
 */
public class TritonElasticSearchClusterConfiguration {
	
	private ImmutableSettings.Builder builder;
	
	private String[] hosts;
	
	public TritonElasticSearchClusterConfiguration() {
		builder = ImmutableSettings.builder();
		builder.put("client.transport.ignore_cluster_name", true);
	}
	
	/**
	 * Set cluster name.
	 * @param name
	 */
	public void setName(String name) {
		builder.put("cluster.name", name);
	}
	
	/**
	 * Set to true to sniff and add cluster machines.
	 * @param sniff
	 */
	public void setSniff(boolean sniff) {
		builder.put("client.transport.sniff", sniff);
	}
	
	/**
	 * Set to true to ignore cluster name validation.
	 * @param ignore
	 */
	public void setIgnoreClusterName(boolean ignore) {
		builder.put("client.transport.ignore_cluster_name", true);
	}
	
	/**
	 * Set host address list.
	 * @param hosts ["127.0.0.1:9300", "127.0.0.2:9300", ..]
	 */
	public void setHosts(String[] hosts) {
		this.hosts = hosts;
	}
	
	/**
	 * Set single host address and port
	 * @param host 127.0.0.1:9300
	 */
	public void setHost(String host) {
		this.hosts = new String[]{ host };
	}
	
	/**
	 * Get host address list.
	 * @return
	 */
	public String[] getHosts() {
		return this.hosts;
	}
	
	/**
	 * Get settings to create client.
	 * @return
	 */
	public Settings getSettings() {
		return builder.build();
	}
	
	/**
	 * Create {@link Client} to connect to the cluster.
	 * @return
	 */
	public Client createClient() {
		TransportClient client = new TransportClient(getSettings());
		for (String host : hosts) {
			client.addTransportAddress(new InetSocketTransportAddress(AddrUtil.getOneAddress(host)));
		}
		return client;
	}

}
