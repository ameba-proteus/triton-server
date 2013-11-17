package com.amebame.proteus.triton;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amebame.triton.client.TritonClient;
import com.amebame.triton.client.memcached.method.DeleteCache;
import com.amebame.triton.client.memcached.method.GetCache;
import com.amebame.triton.client.memcached.method.SetCache;
import com.amebame.triton.exception.TritonClientConnectException;
import com.amebame.triton.exception.TritonClientException;
import com.amebame.triton.json.Json;
import com.amebame.triton.server.TritonServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TritonMemcachedTest {
	
	private TritonServer server;
	private TritonClient client;
	
	private String clusterName = "test";
	
	public TritonMemcachedTest() throws TritonClientConnectException {
		// create a server
		server = new TritonServer();
		server.setConfigPath("src/test/conf/test_memcached.json");
		server.start();
		// create a client
		client = new TritonClient();
		client.open("127.0.0.1", 4848);
	}
	
	@Before
	public void before() throws TritonClientException {
	}
	
	@After
	public void after() throws TritonClientException {
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
	public void testGetSet() throws TritonClientException {
		
		SetCache set = new SetCache();
		set.setCluster(clusterName);
		set.setKey("test1");
		set.setValue(Json.text("text value"));
		
		assertTrue(client.send(set).asBoolean());
		
		GetCache get = new GetCache();
		get.setCluster(clusterName);
		get.setKey("test1");
		
		JsonNode result = client.send(get);
		assertNotNull(result);
		assertEquals("text value", result.asText());
		
	}
	
	@Test
	public void  testExpire() throws TritonClientException, InterruptedException {
		
		SetCache set = new SetCache();
		set.setCluster(clusterName);
		set.setKey("test1");
		set.setValue(Json.text("text value"));
		set.setExpire(1);
		
		assertTrue(client.send(set).asBoolean());
		
		GetCache get = new GetCache();
		get.setCluster(clusterName);
		get.setKey("test1");
		
		JsonNode result = client.send(get);
		assertNotNull(result);
		assertEquals("text value", result.asText());
		
		// wait 1 sec to expire
		Thread.sleep(1000L);
		
		result = client.send(get);
		assertTrue(result.isNull());
		
	}
	
	@Test
	public void testMultiSetGet() throws TritonClientException {
		
		SetCache set = new SetCache();
		set.setCluster(clusterName);
		ObjectNode sets = Json.object();
		ObjectNode struct = Json.object();
		struct.put("child1", "childvalue1");
		struct.put("child2", 1000);
		
		sets.put("key1", "value1");
		sets.put("key2", 2);
		sets.put("key3", struct);
		set.setValue(sets);
		
		assertTrue(client.send(set).asBoolean());
		
		GetCache get = new GetCache();
		get.setCluster(clusterName);
		get.setKeys(Arrays.asList("key1","key2","key3","key4"));
		
		JsonNode result = client.send(get);
		assertNotNull(result);
		assertEquals(3, result.size());
		assertTrue(result.get("key1").isTextual());
		assertEquals("value1", result.get("key1").asText());
		assertTrue(result.get("key2").isNumber());
		assertEquals(2, result.get("key2").asInt());
		assertTrue(result.get("key3").isObject());
		assertEquals(2, result.get("key3").size());
		assertEquals("childvalue1", result.get("key3").get("child1").asText());
		assertEquals(1000, result.get("key3").get("child2").asInt());
		
	}
	
	@Test
	public void testDelete() throws TritonClientException {
		
		SetCache set = new SetCache();
		set.setCluster(clusterName);
		set.setKey("del1");
		set.setValue(Json.text("deleting value"));
		set.setExpire(1);
		
		assertTrue(client.send(set).asBoolean());
		
		GetCache get = new GetCache();
		get.setCluster(clusterName);
		get.setKey("del1");
		
		JsonNode result = client.send(get);
		assertNotNull(result);
		assertEquals("deleting value", result.asText());
		
		DeleteCache del = new DeleteCache();
		del.setCluster(clusterName);
		del.setKey("del1");
		assertTrue(client.send(del).asBoolean());
		
		result = client.send(get);
		assertTrue(result.isNull());
		
	}
	

	protected static final void log(Object ... args) {
		System.out.println(StringUtils.join(args, ' '));
	}
}
