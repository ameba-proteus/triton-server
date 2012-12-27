package com.amebame.proteus.triton;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amebame.triton.client.TritonClient;
import com.amebame.triton.entity.TritonFuture;
import com.amebame.triton.exception.TritonClientConnectException;
import com.amebame.triton.exception.TritonClientException;
import com.amebame.triton.protocol.TritonMessage;
import com.amebame.triton.server.TritonServer;

public class TritonClientTest {
	
	private TritonServer server;
	private TritonClient client;
	
	public TritonClientTest() {
		server = new TritonServer();
		client = new TritonClient();
	}
	
	@Before
	public void start() throws TritonClientConnectException {
		server.start();
		client.open("127.0.0.1",4848);
		assertTrue(client.isOpen());
	}
	
	@After
	public void after() {
		if (client.isOpen()) {
			client.close();
		}
		if (server != null) {
			server.stop();
		}
	}

	@Test
	public void testOpenClose() throws TritonClientConnectException {
		assertTrue(client.isOpen());
		client.close();
		assertFalse(client.isOpen());
	}
	
	@Test(expected=TritonClientException.class)
	public void testSend() throws TritonClientException {
		client.send("test", "string value");
	}
	
	@Test
	public void testHeartbeat() throws TritonClientException {
		TritonFuture future = client.sendAsync("triton.heartbeat");
		TritonMessage result = future.getResult(1000);
		assertTrue(result.isReply());
		assertFalse(result.isError());
	}
	
	@Test
	public void testEcho() throws TritonClientException {
		TritonFuture future = client.sendAsync("triton.echo", "echo value");
		TritonMessage result = future.getResult(1000);
		assertTrue(result.isReply());
		assertFalse(result.isError());
		assertEquals("echo value", result.getBody(String.class));
	}
	
	@Test
	public void  testClose() throws TritonClientException, InterruptedException {
		client.sendAsyncFully("triton.close", null);
		Thread.sleep(100L);
		assertFalse(client.isOpen());
	}
}
