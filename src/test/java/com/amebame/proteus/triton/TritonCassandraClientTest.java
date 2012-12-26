package com.amebame.proteus.triton;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amebame.triton.client.TritonClient;
import com.amebame.triton.client.cassandra.entity.TritonCassandraColumnFamily;
import com.amebame.triton.client.cassandra.entity.TritonCassandraKeyspace;
import com.amebame.triton.client.cassandra.method.CreateColumnFamily;
import com.amebame.triton.client.cassandra.method.CreateKeyspace;
import com.amebame.triton.client.cassandra.method.DropKeyspace;
import com.amebame.triton.client.cassandra.method.ListColumnFamily;
import com.amebame.triton.client.cassandra.method.ListKeyspace;
import com.amebame.triton.exception.TritonClientConnectException;
import com.amebame.triton.exception.TritonClientException;
import com.amebame.triton.exception.TritonException;
import com.amebame.triton.json.Json;
import com.amebame.triton.server.TritonServer;
import com.fasterxml.jackson.databind.JsonNode;

public class TritonCassandraClientTest {
	
	private TritonServer server;
	private TritonClient client;
	
	private String clusterName = "test";
	private String keyspaceName = "triton_test";
	
	public TritonCassandraClientTest() throws TritonClientConnectException {
		// create a server
		server = new TritonServer();
		server.setConfigPath("src/test/conf/test_cassandra.json");
		server.start();
		// create a client
		client = new TritonClient();
		client.open("127.0.0.1", 4848);
	}
	
	@Before
	public void before() throws TritonClientException {
		// create
		CreateKeyspace create = new CreateKeyspace();
		create.setCluster(clusterName);
		create.setKeyspace(keyspaceName);
		create.setReplicationFactor(1);
		JsonNode result = client.send(create);
		assertNotNull(result);
	}
	
	@After
	public void after() throws TritonClientException {
		// drop before
		DropKeyspace drop = new DropKeyspace();
		drop.setCluster(clusterName);
		drop.setKeyspace(keyspaceName);
		JsonNode result = client.send(drop);
		assertNotNull(result);
		// stop the server
		if (server != null) {
			server.stop();
		}
		client.close();
		assertFalse(client.isOpen());
	}

	@Test
	public void testOpenClose() throws TritonClientConnectException {
		assertTrue(client.isOpen());
	}
	
	@Test
	public void listKeyspace() throws TritonException {
		// check list
		ListKeyspace list = new ListKeyspace();
		list.setCluster(clusterName);
		JsonNode body = client.send(list);
		List<TritonCassandraKeyspace> keyspaces = Json.convertAsList(body, TritonCassandraKeyspace.class);
		boolean hasTriton = false;
		for (TritonCassandraKeyspace keyspace : keyspaces) {
			if (keyspace.getName().equals(keyspaceName)) {
				hasTriton = true;
			}
		}
		assertTrue(hasTriton);
	}
	
	@Test
	public void testCreateColumnFamily() throws TritonException {
		// create column family
		CreateColumnFamily create = new CreateColumnFamily();
		create.setCluster(clusterName);
		create.setKeyspace(keyspaceName);
		create.setColumnFamily("test1");
		create.setKeyValidationClass("UTF8Type");
		create.setComparator("UTF8Type");
		create.setDefaultValidationClass("UTF8Type");
		// do create
		JsonNode result = client.send(create);
		assertTrue(result.asBoolean());
		// get result
		ListColumnFamily list = new ListColumnFamily();
		list.setCluster(clusterName);
		list.setKeyspace(keyspaceName);
		result = client.send(list);
		List<TritonCassandraColumnFamily> families = Json.convertAsList(result, TritonCassandraColumnFamily.class);
		boolean hasTestFamily = false;
		for (TritonCassandraColumnFamily family : families) {
			if (family.getName().equals("test1")) {
				hasTestFamily = true;
				log(family);
			}
		}
		assertTrue(hasTestFamily);
	}
	
	private static final void log(Object ... args) {
		System.out.println(StringUtils.join(args, ' '));
	}
}
