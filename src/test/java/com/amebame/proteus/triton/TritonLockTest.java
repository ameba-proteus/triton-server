package com.amebame.proteus.triton;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amebame.triton.client.TritonClient;
import com.amebame.triton.client.TritonClientConfiguration;
import com.amebame.triton.client.lock.method.LockAcquire;
import com.amebame.triton.client.lock.method.LockRelease;
import com.amebame.triton.exception.TritonClientConnectException;
import com.amebame.triton.exception.TritonClientException;
import com.amebame.triton.server.TritonServer;

public class TritonLockTest {
	
	private TritonServer server;
	private TritonClient client;
	
	public TritonLockTest() {
		server = new TritonServer();
		TritonClientConfiguration config = new TritonClientConfiguration();
		config.setBoss(2);
		config.setCommandTimeout(60000L);
		client = new TritonClient(config);
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
	public void testLock() throws TritonClientException, InterruptedException {
		final AtomicInteger value = new AtomicInteger();
		final AtomicBoolean success = new AtomicBoolean(true);
		Thread lockThread1 = new Thread() {
			public void run() {
				try {
					// try lock
					assertTrue(client.send(new LockAcquire("key")).asBoolean());
					// increment one
					assertEquals(1, value.incrementAndGet());
					// sleep 100 ms
					Thread.sleep(500);
					assertEquals(2, value.incrementAndGet());
					// release
					assertTrue(client.send(new LockRelease("key")).asBoolean());
				} catch (Exception e) {
					fail(e.getMessage());
					success.set(false);
				}
			}	
		};
		Thread lockThread2 = new Thread() {
			public void run() {
				try {
					// try lock
					assertTrue(client.send(new LockAcquire("key")).asBoolean());
					// increment one
					assertEquals(3, value.incrementAndGet());
					// release
					assertTrue(client.send(new LockRelease("key")).asBoolean());
				} catch (Exception e) {
					fail(e.getMessage());
					success.set(false);
				}
			}
		};
		lockThread1.start();
		Thread.sleep(100L);
		lockThread2.start();
		try {
			lockThread1.join();
			lockThread2.join();
		} catch (InterruptedException e) {
		}
		assertTrue(success.get());
	}
	
}
