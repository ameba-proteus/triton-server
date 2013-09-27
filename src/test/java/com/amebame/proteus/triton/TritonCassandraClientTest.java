package com.amebame.proteus.triton;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amebame.triton.client.TritonClient;
import com.amebame.triton.client.cassandra.entity.TritonCassandraKeyspace;
import com.amebame.triton.client.cassandra.entity.TritonCassandraTable;
import com.amebame.triton.client.cassandra.method.BatchOperation;
import com.amebame.triton.client.cassandra.method.BatchOperationMode;
import com.amebame.triton.client.cassandra.method.BatchUpdate;
import com.amebame.triton.client.cassandra.method.CreateKeyspace;
import com.amebame.triton.client.cassandra.method.CreateTable;
import com.amebame.triton.client.cassandra.method.DropKeyspace;
import com.amebame.triton.client.cassandra.method.GetColumns;
import com.amebame.triton.client.cassandra.method.ListKeyspace;
import com.amebame.triton.client.cassandra.method.ListTable;
import com.amebame.triton.client.cassandra.method.RemoveColumns;
import com.amebame.triton.client.cassandra.method.SetColumns;
import com.amebame.triton.client.cassandra.method.TruncateTable;
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
	public void testCreateTable() throws TritonException {
		// create column family
		CreateTable create = new CreateTable();
		create.setCluster(clusterName);
		create.setKeyspace(keyspaceName);
		create.setTable("test1");
		create.setKeyType("text");
		create.setColumnType("text");
		// do create
		JsonNode result = client.send(create);
		assertTrue(result.asBoolean());
		// get result
		ListTable list = new ListTable();
		list.setCluster(clusterName);
		list.setKeyspace(keyspaceName);
		result = client.send(list);
		List<TritonCassandraTable> families = Json.convertAsList(result, TritonCassandraTable.class);
		boolean hasTestFamily = false;
		for (TritonCassandraTable family : families) {
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
		CreateTable create = new CreateTable();
		create.setCluster(clusterName);
		create.setKeyspace(keyspaceName);
		create.setTable(familyName);
		create.setKeyType("text");
		create.setColumnType("text");
		JsonNode result = client.send(create);
		assertTrue(result.asBoolean());
		
		// set data
		SetColumns set = new SetColumns();
		set.setCluster(clusterName);
		set.setKeyspace(keyspaceName);
		set.setTable(familyName);
		
		// rows
		ObjectNode rows = Json.object();
		rows
		.putObject("row1")
		.put("column1", "value1")
		.put("column2", "value2")
		.put("column2_1", "value2_1")
		.put("column2_2", "value2_2")
		.put("column4", 100)
		.set("column3", Json.object().put("name1", "valuechild").put("name2", 1000))
		;
		set.setRows(rows);
		
		assertTrue(client.send(set).asBoolean());
		
		// get row
		GetColumns get = new GetColumns();
		get.setCluster(clusterName);
		get.setKeyspace(keyspaceName);
		get.setTable(familyName);
		// single key
		get.setKey(Json.text("row1"));
		// single column
		get.setColumn(Json.text("column1"));
		
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
		
		// range column (start)
		JsonNode start = Json.text("column2");
		ObjectNode range = Json.object();
		range.put("start", start);
		get.setColumns(range);
		result = client.send(get);
		assertEquals(5, result.size());
		assertEquals("column2", result.get(0).get("column").asText());
		assertEquals("column2_1", result.get(1).get("column").asText());
		assertEquals("column2_2", result.get(2).get("column").asText());
		assertEquals("column3", result.get(3).get("column").asText());
		assertEquals("valuechild", result.get(3).get("value").get("name1").asText());
		assertEquals(1000, result.get(3).get("value").get("name2").asInt());
		assertEquals("column4", result.get(4).get("column").asText());
		assertEquals(100, result.get(4).get("value").asInt());
		
		// range column (end)
		range = Json.object();
		JsonNode end = Json.text("column3");
		range.put("end", end);
		get.setColumns(range);
		result = client.send(get);
		assertEquals(5, result.size());
		assertEquals("column1", result.get(0).get("column").asText());
		assertEquals("column2", result.get(1).get("column").asText());
		assertEquals("column2_1", result.get(2).get("column").asText());
		assertEquals("column2_2", result.get(3).get("column").asText());
		assertEquals("column3", result.get(4).get("column").asText());
		assertEquals("valuechild", result.get(4).get("value").get("name1").asText());
		assertEquals(1000, result.get(4).get("value").get("name2").asInt());
		
		// range column (start and end)
		range.put("start", start);
		get.setColumns(range);
		result = client.send(get);
		assertEquals(4, result.size());
		assertEquals("column2", result.get(0).get("column").asText());
		assertEquals("column2_1", result.get(1).get("column").asText());
		assertEquals("column2_2", result.get(2).get("column").asText());
		assertEquals("column3", result.get(3).get("column").asText());
		assertEquals("valuechild", result.get(3).get("value").get("name1").asText());
		assertEquals(1000, result.get(3).get("value").get("name2").asInt());
		
		// exclusive range
		ObjectNode startObj = Json.object();
		JsonNode startValue = Json.text("column2");
		startObj.put("value", startValue);
		JsonNode exclusive = Json.bool(true);
		startObj.put("value", startValue);
		startObj.put("exclusive", exclusive);
		ObjectNode endObj = Json.object();
		JsonNode endValue = Json.text("column4");
		endObj.put("value", endValue);
		endObj.put("exclusive", exclusive);
		range = Json.object();
		range.put("start", startObj);
		range.put("end", endObj);
		get.setColumns(range);
		result = client.send(get);
		assertEquals(3, result.size());
		assertEquals("column2_1", result.get(0).get("column").asText());
		assertEquals("column2_2", result.get(1).get("column").asText());
		assertEquals("column3", result.get(2).get("column").asText());
		assertEquals("valuechild", result.get(2).get("value").get("name1").asText());
		assertEquals(1000, result.get(2).get("value").get("name2").asInt());
		
		// start with
		JsonNode startWith = Json.text("column2");
		range = Json.object();
		range.put("startWith", startWith);
		get.setColumns(range);
		result = client.send(get);
		assertEquals(3, result.size());
		
		// all columns
		get.setColumns(null);
		result = client.send(get);
		assertEquals(6, result.size());
		assertEquals("value1", result.get("column1").asText());
		assertEquals("value2", result.get("column2").asText());
		assertEquals("valuechild", result.get("column3").get("name1").asText());
		assertEquals(1000, result.get("column3").get("name2").asInt());
		assertEquals(100, result.get("column4").asInt());
	
		// TODO reverse order
		
		// put multiple rows
		rows = Json.object();
		get.setColumns(null);
		
		ObjectNode row2 = rows.putObject("row2");
		row2.put("column1", Json.text("value1"));
		row2.put("column2", Json.text("value2"));
		row2.put("column3", Json.text("value3"));
		
		ObjectNode row3 = rows.putObject("row3");
		row3.put("column1", Json.text("value1"));
		row3.put("column3", Json.text("value3"));
		row3.put("column4", Json.text("value4"));
		
		ObjectNode row4 = rows.putObject("row4");
		row4.put("column1", Json.text("value1"));
		
		ObjectNode row5 = rows.putObject("row5");
		row5.put("column1", Json.text("value1"));
		
		set.setRows(rows);
		
		assertTrue(client.send(set).asBoolean());
		
		// multiple key
		get.setKeys(Json.array().add("row2").add("row3").add("row6"));
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
		
		// key range ()
		range = Json.object();
		start = Json.text("row3");
		range.put("start", start);
		end = Json.text("row5");
		range.put("end", end);
		get.setKeys(range);
		result = client.send(get);
//		assertEquals(3, result.size()); // result will be changed according to what partitioner you use.
		
	}
	
	@Test
	public void testRemove() throws TritonException {
		
		String familyName = "test_remove";
		
		// creating test family
		CreateTable create = new CreateTable();
		create.setCluster(clusterName);
		create.setKeyspace(keyspaceName);
		create.setTable(familyName);
		create.setKeyType("text");
		create.setColumnType("text");
		assertTrue(client.send(create, Boolean.class));
		
		// rows
		SetColumns set = new SetColumns();
		set.setCluster(clusterName);
		set.setKeyspace(keyspaceName);
		set.setTable(familyName);
		
		ObjectNode rows = Json.object();
		ObjectNode columns = rows.putObject("row1");
		columns.put("column1", Json.text("value11"));
		columns.put("column2", Json.text("value12"));
		columns.put("column3", Json.text("value13"));
		columns = rows.putObject("row2");
		columns.put("column1", Json.text("value21"));
		columns.put("column2", Json.text("value22"));
		columns.put("column3", Json.text("value23"));
		columns = rows.putObject("row3");
		columns.put("column1", Json.text("value31"));
		columns.put("column2", Json.text("value32"));
		columns.put("column3", Json.text("value33"));
		set.setRows(rows);
		assertTrue(client.send(set, Boolean.class));
		
		GetColumns get = new GetColumns();
		get.setCluster(clusterName);
		get.setKeyspace(keyspaceName);
		get.setTable(familyName);
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
		remove.setTable(familyName);
		ObjectNode removes = Json.object();
		// only single row with columns
		removes.put("row1", Json.array().add("column1").add("column2"));
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
		
		removes = Json.object();
		// remove entire row
		remove.setKey("row2");
		remove.setRows(null);
		assertTrue(client.send(remove, Boolean.class));
		
		get = new GetColumns();
		get.setCluster(clusterName);
		get.setKeyspace(keyspaceName);
		get.setTable(familyName);
		//get.setKeys(Json.array().add("row1").add("row2").add("row3"));
		result = client.send(get);
		assertEquals(2, result.size());
		assertFalse(result.has("row2"));
		assertTrue(result.has("row1"));
		assertTrue(result.has("row3"));
		
		// remove multiple row columns
		removes = Json.object();
		removes.putArray("row1").add("column3");
		removes.putArray("row3").add("column2").add("column3");
		remove.setRows(removes);
		assertTrue(client.send(remove, Boolean.class));
		
		result = client.send(get);
		assertEquals(1, result.size());
		assertFalse(result.has("row1"));
		assertTrue(result.has("row3"));
		assertEquals(1, result.get("row3").size());
		assertTrue(result.get("row3").has("column1"));
		
		log(result);
	}
	
	@Test
	public void testTruncate() throws TritonException {
		
		String familyName = "test_truncate";
		
		// creating test family
		CreateTable create = new CreateTable();
		create.setCluster(clusterName);
		create.setKeyspace(keyspaceName);
		create.setTable(familyName);
		create.setKeyType("text");
		create.setColumnType("text");
		assertTrue(client.send(create, Boolean.class));
		
		// rows
		SetColumns set = new SetColumns();
		set.setCluster(clusterName);
		set.setKeyspace(keyspaceName);
		set.setTable(familyName);
		
		ObjectNode rows = Json.object();
		ObjectNode columns = rows.putObject("row1");
		columns.put("column1", Json.text("value11"));
		columns = rows.putObject("row2");
		columns.put("column1", Json.text("value21"));
		columns = rows.putObject("row3");
		columns.put("column1", Json.text("value31"));
		set.setRows(rows);
		assertTrue(client.send(set, Boolean.class));
		
		GetColumns get = new GetColumns();
		get.setCluster(clusterName);
		get.setKeyspace(keyspaceName);
		get.setTable(familyName);
		
		JsonNode result = client.send(get);
		assertEquals(3, result.size());
		
		// truncate
		TruncateTable truncate = new TruncateTable();
		truncate.setCluster(clusterName);
		truncate.setKeyspace(keyspaceName);
		truncate.setTable(familyName);
		assertTrue(client.send(truncate, Boolean.class));
		
		result = client.send(get);
		assertEquals(0, result.size());
	}
	
	@Test
	public void testBatch() throws TritonException {
		
		// creating batch test family
		CreateTable create = new CreateTable();
		create.setCluster(clusterName);
		create.setKeyspace(keyspaceName);
		create.setTable("batch1");
		create.setKeyType("text");
		create.setColumnType("text");
		assertTrue(client.send(create, Boolean.class));
		
		// creating second batch test family
		create = new CreateTable();
		create.setCluster(clusterName);
		create.setKeyspace(keyspaceName);
		create.setTable("batch2");
		create.setKeyType("text");
		create.setColumnType("int");
		assertTrue(client.send(create, Boolean.class));
		
		BatchUpdate batch = new BatchUpdate();
		batch.setCluster(clusterName);
		batch.setKeyspace(keyspaceName);
		
		// return false if batch is empty
		assertFalse(client.send(batch, Boolean.class));
		
		BatchOperation operation = batch.addSet();
		operation.setTable("batch1");
		
		ObjectNode rows = operation.createRows();
		rows.putObject("row1")
		.put("column1", "value11")
		.put("column2", "value12");
		rows.putObject("row2").put("column1", "value21");

		operation = batch.addSet();
		operation.setTable("batch2");
		operation.setMode(BatchOperationMode.set);

		rows = operation.createRows();
		rows.putObject("row1").put("100", "value100");
		rows.putObject("row2").put("200", "value200");

		assertTrue(client.send(batch, Boolean.class));
		
		GetColumns get = new GetColumns();
		get.setCluster(clusterName);
		get.setKeyspace(keyspaceName);
		get.setTable("batch1");
		get.setKey(Json.text("row1"));
		
		JsonNode node = client.send(get);
		assertTrue(node.has("column1"));
		assertTrue(node.has("column2"));
		assertEquals("value11", node.get("column1").asText());
		assertEquals("value12", node.get("column2").asText());
		assertEquals(2, node.size());
		
		get.setKey(Json.text("row2"));
		node = client.send(get);
		assertTrue(node.has("column1"));
		assertEquals("value21", node.get("column1").asText());
		assertEquals(1, node.size());
		
		get.setTable("batch2");
		get.setKey(Json.text("row1"));
		node = client.send(get);
		assertTrue(node.has("100"));
		assertEquals("value100", node.get("100").asText());
		assertEquals(1, node.size());
		
		get.setKey(Json.text("row2"));
		node = client.send(get);
		assertTrue(node.has("200"));
		assertEquals("value200", node.get("200").asText());
		assertEquals(1, node.size());
		
		// start removing with batch operation
		batch = new BatchUpdate();
		batch.setCluster(clusterName);
		batch.setKeyspace(keyspaceName);
		
		operation = batch.addRemove();
		operation.setTable("batch1");
		rows = operation.createRows();
		rows.putArray("row1").add("column1");
		operation.setKey("row2");
		
		operation = batch.addRemove();
		operation.setTable("batch2");
		operation.setKey("row1");
		
		assertTrue(client.send(batch, Boolean.class));
		
		get.setTable("batch1");
		get.setKey(Json.text("row1"));
		
		node = client.send(get);
		assertEquals(1, node.size());
		
		get.setKey(Json.text("row2"));
		node = client.send(get);
		assertTrue(node.isNull());
		
		get.setTable("batch2");
		get.setKey(Json.text("row1"));
		node = client.send(get);
		assertTrue(node.isNull());
		
	}
	
	private static final void log(Object ... args) {
		System.out.println(StringUtils.join(args, ' '));
	}
}
