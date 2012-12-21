package com.amebame.proteus.triton;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amebame.triton.client.TritonClient;
import com.amebame.triton.entity.TritonFuture;
import com.amebame.triton.exception.TritonConnectException;
import com.amebame.triton.protocol.TritonMessage;
import com.amebame.triton.server.TritonServer;

public class TritonClientTest {
	
	private TritonServer server;
	private TritonClient client;
	
	public TritonClientTest() {
		server = new TritonServer();
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
	public void testSend() throws TritonConnectException {
		client.open("127.0.0.1", 4848);
		TritonFuture future = client.send("test", "string value");
		TritonMessage result = future.getResult(1000);
		assertTrue(result.isError());
		client.close();
	}
	
	@Test
	public void testHeartbeat() throws TritonConnectException {
		client.open("127.0.0.1", 4848);
		TritonFuture future = client.send("triton.heartbeat");
		TritonMessage result = future.getResult(1000);
		assertTrue(result.isReply());
		assertFalse(result.isError());
		client.close();
	}
	
	@Test
	public void testEcho() throws TritonConnectException {
		client.open("127.0.0.1", 4848);
		TritonFuture future = client.send("triton.echo", "echo value");
		TritonMessage result = future.getResult(1000);
		assertTrue(result.isReply());
		assertFalse(result.isError());
		assertEquals("echo value", result.getBody(String.class));
		client.close();
	}
}
