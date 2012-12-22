package com.amebame.proteus.triton;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amebame.triton.client.TritonClient;
import com.amebame.triton.entity.TritonFuture;
import com.amebame.triton.exception.TritonConnectException;
import com.amebame.triton.exception.TritonException;
import com.amebame.triton.protocol.TritonMessage;
import com.amebame.triton.server.TritonServer;
import com.amebame.triton.service.cassandra.entity.CreateKeyspace;
import com.amebame.triton.service.cassandra.entity.DropKeyspace;

public class TritonCassandraClientTest {
	
	private TritonServer server;
	private TritonClient client;
	
	public TritonCassandraClientTest() {
		server = new TritonServer();
		server.setConfigPath("src/test/conf/test_cassandra.json");
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
	
	@Test
	public void testCreateKeyspace() throws TritonException {
		client.open("127.0.0.1", 4848);
		// drop before
		DropKeyspace drop = new DropKeyspace();
		drop.setCluster("Test Cluster");
		drop.setKeyspace("triton_test");
		TritonFuture future = client.send("cassandra.keyspace.drop", drop);
		future.await();
		
		// check list
		
		// create
		CreateKeyspace create = new CreateKeyspace();
		create.setCluster("Test Cluster");
		create.setKeyspace("triton_test");
		create.setReplicationFactor(1);
		future = client.send("cassandra.keyspace.create", create);
		TritonMessage message = future.getResult();
		assertTrue(message.isReply());
		assertFalse(message.isError());
		
		// check list
	}
	
	@Test
	public void testCreateColumnFamily() {
	}
	
}
