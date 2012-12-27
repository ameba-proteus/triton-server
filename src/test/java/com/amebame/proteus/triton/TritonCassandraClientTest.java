package com.amebame.proteus.triton;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.amebame.triton.client.cassandra.method.GetColumns;
import com.amebame.triton.client.cassandra.method.ListColumnFamily;
import com.amebame.triton.client.cassandra.method.ListKeyspace;
import com.amebame.triton.client.cassandra.method.RemoveColumns;
import com.amebame.triton.client.cassandra.method.SetColumns;
import com.amebame.triton.exception.TritonClientConnectException;
import com.amebame.triton.exception.TritonClientException;
import com.amebame.triton.exception.TritonException;
import com.amebame.triton.json.Json;
import com.amebame.triton.server.TritonServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
	
	@Test
	public void testSetGet() throws TritonException {
		
		String familyName = "test_getset";
		
		// creating test family
		CreateColumnFamily create = new CreateColumnFamily();
		create.setCluster(clusterName);
		create.setKeyspace(keyspaceName);
		create.setColumnFamily(familyName);
		create.setKeyValidationClass("UTF8Type");
		create.setComparator("UTF8Type");
		create.setDefaultValidationClass("UTF8Type");
		JsonNode result = client.send(create);
		assertTrue(result.asBoolean());
		
		// set data
		SetColumns set = new SetColumns();
		set.setCluster(clusterName);
		set.setKeyspace(keyspaceName);
		set.setColumnFamily(familyName);
		
		// rows
		Map<String, Map<String, JsonNode>> rows = new HashMap<>();
		Map<String, JsonNode> columns = new HashMap<>();
		columns.put("column1", Json.text("value1"));
		columns.put("column2", Json.text("value2"));
		columns.put("column3", Json.object().put("name1", "valuechild").put("name2", 1000));
		columns.put("column4", Json.number(100));
		rows.put("row1", columns);
		set.setRows(rows);
		
		assertTrue(client.send(set).asBoolean());
		
		// get row
		GetColumns get = new GetColumns();
		get.setCluster(clusterName);
		get.setKeyspace(keyspaceName);
		get.setColumnFamily(familyName);
		// single key
		get.setKey(Json.text("row1"));
		// single column
		get.setColumns(Json.text("column1"));
		
		// get column
		result = client.send(get);
		assertNotNull(result);
		assertEquals("value1", result.asText());
		
		// multiple column
		get.setColumns(Json.array().add("column1").add("column3").add("column4"));
		result = client.send(get);
		assertEquals(3, result.size());
		assertEquals("value1", result.get("column1").asText());
		assertEquals(2, result.get("column3").size());
		assertEquals("valuechild", result.get("column3").get("name1").asText());
		assertEquals(1000, result.get("column3").get("name2").asInt());
		assertEquals(100, result.get("column4").asInt());
		assertFalse(result.has("value2"));
		
		// range column
		JsonNode start = Json.text("column3");
		ObjectNode range = Json.object();
		range.put("start", start);
		get.setColumns(range);
		result = client.send(get);
		assertEquals(2, result.size());
		assertEquals("column3", result.get(0).get("column").asText());
		assertEquals("valuechild", result.get(0).get("value").get("name1").asText());
		assertEquals(1000, result.get(0).get("value").get("name2").asInt());
		assertEquals("column4", result.get(1).get("column").asText());
		assertEquals(100, result.get(1).get("value").asInt());
		
		// all columns
		get.setColumns(null);
		result = client.send(get);
		assertEquals(4, result.size());
		assertEquals("value1", result.get("column1").asText());
		assertEquals("value2", result.get("column2").asText());
		assertEquals("valuechild", result.get("column3").get("name1").asText());
		assertEquals(1000, result.get("column3").get("name2").asInt());
		assertEquals(100, result.get("column4").asInt());
		
		// TODO reverse order
		
		// TODO exclusive range
		
		// put multiple rows
		rows.clear();
		Map<String, JsonNode> columns2 = new HashMap<>();
		columns2.put("column1", Json.text("value1"));
		columns2.put("column2", Json.text("value2"));
		columns2.put("column3", Json.text("value3"));
		
		Map<String, JsonNode> columns3 = new HashMap<>();
		columns3.put("column1", Json.text("value1"));
		columns3.put("column3", Json.text("value3"));
		columns3.put("column4", Json.text("value4"));
		
		rows.put("row2", columns2);
		rows.put("row3", columns3);
		
		assertTrue(client.send(set).asBoolean());
		
		// set key range
		get.setKeys(Json.array().add("row2").add("row3").add("row4"));
		
		result = client.send(get);
		assertEquals(2, result.size());
		assertTrue(result.has("row2"));
		assertTrue(result.has("row3"));
		assertEquals(3, result.get("row2").size());
		assertEquals("value1", result.get("row2").get("column1").asText());
		assertEquals("value2", result.get("row2").get("column2").asText());
		assertEquals("value3", result.get("row2").get("column3").asText());
		assertEquals(3, result.get("row3").size());
		assertEquals("value1", result.get("row3").get("column1").asText());
		assertEquals("value3", result.get("row3").get("column3").asText());
		assertEquals("value4", result.get("row3").get("column4").asText());
		
		// set column range
		
	}
	
	@Test
	public void testRemove() throws TritonException {
		
		String familyName = "test_remove";
		
		// creating test family
		CreateColumnFamily create = new CreateColumnFamily();
		create.setCluster(clusterName);
		create.setKeyspace(keyspaceName);
		create.setColumnFamily(familyName);
		create.setKeyValidationClass("UTF8Type");
		create.setComparator("UTF8Type");
		create.setDefaultValidationClass("UTF8Type");
		assertTrue(client.send(create, Boolean.class));
		
		// rows
		SetColumns set = new SetColumns();
		Map<String, Map<String, JsonNode>> rows = new HashMap<>();
		Map<String, JsonNode> columns = new HashMap<>();
		set.setCluster(clusterName);
		set.setKeyspace(keyspaceName);
		set.setColumnFamily(familyName);
		columns.put("column1", Json.text("value11"));
		columns.put("column2", Json.text("value12"));
		columns.put("column3", Json.text("value13"));
		rows.put("row1", columns);
		columns = new HashMap<>();
		columns.put("column1", Json.text("value21"));
		columns.put("column2", Json.text("value22"));
		columns.put("column3", Json.text("value23"));
		rows.put("row2", columns);
		columns = new HashMap<>();
		columns.put("column1", Json.text("lalue31"));
		columns.put("column2", Json.text("value32"));
		columns.put("column3", Json.text("value33"));
		rows.put("row3", columns);
		set.setRows(rows);
		assertTrue(client.send(set, Boolean.class));
		
		GetColumns get = new GetColumns();
		get.setCluster(clusterName);
		get.setKeyspace(keyspaceName);
		get.setColumnFamily(familyName);
		JsonNode result = client.send(get);
		assertEquals(3, result.size());
		assertEquals("value11", result.get("row1").get("column1").asText());
		assertEquals("value12", result.get("row1").get("column2").asText());
		assertEquals("value13", result.get("row1").get("column3").asText());
		assertEquals(3, result.get("row2").size());
		assertEquals(3, result.get("row3").size());
		
		RemoveColumns remove = new RemoveColumns();
		remove.setCluster(clusterName);
		remove.setKeyspace(keyspaceName);
		remove.setColumnFamily(familyName);
		Map<String, List<String>> removes = new HashMap<>();
		// only single row with columns
		removes.put("row1", Arrays.asList("column1", "column2"));
		remove.setRows(removes);
		assertTrue(client.send(remove, Boolean.class));
		
		result = client.send(get);
		assertEquals(3, result.size());
		assertEquals(1, result.get("row1").size());
		assertFalse(result.get("row1").has("column1"));
		assertFalse(result.get("row1").has("column2"));
		assertTrue(result.get("row1").has("column3"));
		assertEquals(3, result.get("row2").size());
		assertEquals(3, result.get("row3").size());
		
		removes.clear();
		// remove entire row
		removes.put("row2", new ArrayList<String>());
		assertTrue(client.send(remove, Boolean.class));
		
		result = client.send(get);
		assertEquals(2, result.size());
		assertFalse(result.has("row2"));
		assertTrue(result.has("row1"));
		assertTrue(result.has("row3"));
		
		// remove multiple row columns
		removes.clear();
		removes.put("row1", Arrays.asList("column3"));
		removes.put("row3", Arrays.asList("column2","column3"));
		assertTrue(client.send(remove, Boolean.class));
		
		result = client.send(get);
		assertEquals(1, result.size());
		assertFalse(result.has("row1"));
		assertTrue(result.has("row3"));
		assertEquals(1, result.get("row3").size());
		assertTrue(result.get("row3").has("column1"));
		
		log(result);
	}
	
	private static final void log(Object ... args) {
		System.out.println(StringUtils.join(args, ' '));
	}
}
