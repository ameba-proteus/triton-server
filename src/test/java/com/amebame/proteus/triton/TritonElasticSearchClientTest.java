package com.amebame.proteus.triton;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amebame.triton.client.TritonClient;
import com.amebame.triton.client.elasticsearch.method.DeleteDocument;
import com.amebame.triton.client.elasticsearch.method.GetDocument;
import com.amebame.triton.client.elasticsearch.method.IndexDocument;
import com.amebame.triton.client.elasticsearch.method.MultiGetDocument;
import com.amebame.triton.client.elasticsearch.method.SearchDocument;
import com.amebame.triton.client.elasticsearch.method.UpdateDocument;
import com.amebame.triton.exception.TritonClientConnectException;
import com.amebame.triton.exception.TritonClientException;
import com.amebame.triton.json.Json;
import com.amebame.triton.server.TritonServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TritonElasticSearchClientTest {
	
	private TritonServer server;

	private TritonClient client;
	
	private String clusterName = "test";
	
	private String indexName = "tritontest";
	
	public TritonElasticSearchClientTest() throws TritonClientConnectException {
		// create a server
		server = new TritonServer();
		server.setConfigPath("src/test/conf/test_elasticsearch.json");
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
	public void testIndex() throws TritonClientException {
		
		IndexDocument index = new IndexDocument();
		index.setCluster(clusterName);
		index.setIndex(indexName);
		index.setType("test1");
		index.setId("1");
		
		ObjectNode node = Json.object();
		node
		.put("prop1", "value1")
		.put("prop2", 100)
		;

		index.setSource(node);
		
		JsonNode result = client.send(index);
		assertNotNull(result);
		assertEquals("1", result.get("id").asText());
		
	}
	
	@Test
	public void testGetNull() throws TritonClientException {
		GetDocument get = new GetDocument();
		get.setCluster(clusterName);
		get.setIndex(indexName);
		get.setType("test1");
		get.setId("xxx");
		
		JsonNode result = client.send(get);
		assertNotNull(result);
		assertTrue(result.isNull());
	}
	
	@Test
	public void testGet() throws TritonClientException {

		IndexDocument index = new IndexDocument();
		index.setCluster(clusterName);
		index.setIndex(indexName);
		index.setType("test1");
		index.setId("2");
		
		ObjectNode node = Json.object();
		node
		.put("prop1", "value1")
		.put("prop2", 1000)
		.put("prop3", Json.object()
				.put("iprop1", 2000)
				.put("iprop2", "value2"));

		index.setSource(node);
		
		JsonNode result = client.send(index);
		assertNotNull(result);
		assertEquals("2", result.get("id").asText());
		
		GetDocument get = new GetDocument();
		get.setCluster(clusterName);
		get.setIndex(indexName);
		get.setType("test1");
		get.setId("2");
		
		result = client.send(get);
		assertNotNull(result);
		assertEquals("2", result.get("id").asText());
		assertTrue(result.has("source"));
		
		JsonNode source = result.get("source");
		assertEquals("value1", source.get("prop1").asText());
		assertEquals(1000, source.get("prop2").asInt());
		assertTrue(source.has("prop3"));
		assertEquals(2000, source.get("prop3").get("iprop1").asInt());
		assertEquals("value2", source.get("prop3").get("iprop2").asText());
		
	}
	
	@Test
	public void testMultiGet() throws TritonClientException {

		IndexDocument index = new IndexDocument();
		index.setCluster(clusterName);
		index.setIndex(indexName);
		index.setType("test1");
		index.setId("10");
		index.setSource(Json.object().put("prop1", "value10"));
		;
		JsonNode result = client.send(index);
		assertNotNull(result);
		assertEquals("10", result.get("id").asText());

		index.setId("11");
		index.setSource(Json.object().put("prop1", "value11"));
		result = client.send(index);
		assertNotNull(result);
		assertEquals("11", result.get("id").asText());

		index.setId("12");
		index.setSource(Json.object().put("prop1", "value12"));
		result = client.send(index);
		assertNotNull(result);
		assertEquals("12", result.get("id").asText());

		MultiGetDocument multiGet = new MultiGetDocument();
		multiGet.setCluster(clusterName);
		multiGet.setIndex(indexName);
		multiGet.setType("test1");
		multiGet.setIds(Arrays.asList("10","11","12"));
		
		result = client.send(multiGet);
		assertNotNull(result);
		
		assertTrue(result.has("items"));
		
		JsonNode items = result.get("items");

		assertTrue(items.has("10"));
		assertTrue(items.has("11"));
		assertTrue(items.has("12"));

		assertTrue(result.get("items").has("12"));

	}
	
	@Test
	public void testUpdate() throws TritonClientException {
		
		IndexDocument index = new IndexDocument();
		index.setCluster(clusterName);
		index.setIndex(indexName);
		index.setType("test1");
		index.setId("30");
		index.setSource(Json.object()
				.put("prop1", "value10")
				.put("prop2", 500)
		);
		JsonNode result = client.send(index);
		assertNotNull(result);
		assertEquals("30", result.get("id").asText());

		UpdateDocument update = new UpdateDocument();
		update.setCluster(clusterName);
		update.setIndex(indexName);
		update.setType("test1");
		update.setId("30");
		update.setDoc(Json.object()
				.put("prop1", "value11")
				.put("prop2", 600)
		);
		
		result = client.send(update);
		
	}
	
	@Test
	public void testDelete() throws TritonClientException {

		IndexDocument index = new IndexDocument();
		index.setCluster(clusterName);
		index.setIndex(indexName);
		index.setType("test1");
		index.setId("40");
		index.setSource(Json.object()
				.put("prop1", "value10")
				.put("prop2", 1000)
		);
		JsonNode result = client.send(index);
		assertNotNull(result);
		assertEquals("40", result.get("id").asText());
		
		DeleteDocument delete = new DeleteDocument();
		delete.setCluster(clusterName);
		delete.setIndex(indexName);
		delete.setType("test1");
		delete.setId("40");
		
		result = client.send(delete);
		assertNotNull(result);
		
		GetDocument get = new GetDocument();
		get.setCluster(clusterName);
		get.setIndex(indexName);
		get.setType("test1");
		get.setId("40");
		
		result = client.send(get);
		assertNotNull(result);
		assertTrue(result.isNull());
		
	}
	
	@Test
	public void testSearch() throws TritonClientException {
		
		client.send(index("100", Json.object().put("prop1", "search test word part 1")));
		client.send(index("101", Json.object().put("prop1", "search test word part 2")));
		client.send(index("102", Json.object().put("prop1", "search test word part 3")));
		client.send(index("110", Json.object().put("prop1", "another search test 1")));
		client.send(index("111", Json.object().put("prop1", "another search test 2")));
		
		// Search by query.
		SearchDocument search = new SearchDocument();
		search.setCluster(clusterName);
		search.setIndex(indexName);
		search.setType("test1");
		
		ObjectNode query = Json.object();
		query.put("term", Json.object().put("prop1", "word"));

		search.setQuery(query);

		JsonNode result = client.send(search);
		assertNotNull(result);
		assertEquals(3, result.get("total").asInt());
		
		// Search by filter.
		search.setQuery(null);
		search.setFilter(query);
		
		client.send(search);
		assertNotNull(result);
		assertEquals(3, result.get("total").asInt());

	}
	
	private IndexDocument index(String id, JsonNode source) {
		IndexDocument index = new IndexDocument();
		index.setCluster(clusterName);
		index.setIndex(indexName);
		index.setType("test1");
		index.setId(id);
		index.setSource(source);
		return index;
	}
	
	/*
	private static final void log(Object ... args) {
		System.out.println(StringUtils.join(args, ' '));
	}
	*/
}
