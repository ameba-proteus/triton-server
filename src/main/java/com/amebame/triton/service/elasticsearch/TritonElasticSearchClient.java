package com.amebame.triton.service.elasticsearch;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;

import com.amebame.triton.config.TritonElasticSearchClusterConfiguration;
import com.amebame.triton.config.TritonElasticSearchConfiguration;
import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.server.TritonCleaner;

/**
 * {@link TritonElasticSearchClient} provides {@link Client} instances.
 */
public class TritonElasticSearchClient implements TritonCleaner {
	
	private static final Logger log = LogManager.getLogger(TritonElasticSearchClient.class);

	// configuration
	private TritonElasticSearchConfiguration config;
	
	// client map which are registered
	private Map<String, Client> clients;

	@Inject
	public TritonElasticSearchClient(TritonElasticSearchConfiguration config) {
		this.config = config;
		buildClients();
	}
	
	/**
	 * Get client which has connected to the cluster.
	 * @param key
	 * @return
	 */
	public Client getClient(String key) {
		Client client = clients.get(key);
		if (client == null) {
			throw new TritonElasticSearchException(TritonErrors.elasticsearch_no_cluster, "ElasticSearch cluster " + key + " is not configured.");
		}
		return client;
	}
	
	/**
	 * Build cluster clients.
	 */
	private void buildClients() {
		clients = new HashMap<>();
		for (String key : config.getClusterKeys()) {
			TritonElasticSearchClusterConfiguration clusterConf = config.getClusterConfiguration(key);
			clients.put(key, clusterConf.createClient());
			String hostsLine = StringUtils.join(clusterConf.getHosts(), ' ');
			log.info("creating the elastic search client which connect to {}", hostsLine);
		}
	}
	
	@Override
	public void clean() {
		for (Client client : clients.values()) {
			client.close();
		}
	}

}
