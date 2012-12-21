package com.amebame.proteus.triton;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amebame.triton.client.TritonClient;
import com.amebame.triton.config.TritonCassandraClusterConfiguration;
import com.amebame.triton.config.TritonCassandraConfiguration;
import com.amebame.triton.config.TritonServerConfiguration;
import com.amebame.triton.exception.TritonConnectException;
import com.amebame.triton.server.TritonServer;

public class TritonCassandraClientTest {
	
	private TritonServer server;
	private TritonClient client;
	private TritonServerConfiguration config;
	
	public TritonCassandraClientTest() {
		server = new TritonServer();
		config = new TritonServerConfiguration();
		config.setCassandra(
				new TritonCassandraConfiguration()
				.setCluster(
						"Test Cluster",
						new TritonCassandraClusterConfiguration()
						.setSeeds("127.0.0.1")
				)
		);
		server.start();
	}
	
	@Before
	public void start() {
		client = new TritonClient();
	}
	
	@After
	public void after() {
		if (server != null) {
			server.stop();
		}
	}

	@Test
	public void testOpenClose() throws TritonConnectException {
		client.open("127.0.0.1", 4848);
		assertTrue(client.isOpen());
		client.close();
		assertFalse(client.isOpen());
	}
	
}
