package com.amebame.triton.entity;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.amebame.triton.protocol.TritonMessage;

/**
 * TritonFuture
 */
public class TritonFuture {
	
	private TritonCall call;
	
	private TritonMessage result;
	
	private CountDownLatch latch;

	public TritonFuture(TritonCall call) {
		this.call = call;
		this.latch = new CountDownLatch(1);
	}
	
	/**
	 * Get the callID of the call which sent to the server.
	 * @return
	 */
	public int getCallId() {
		return call.getCallId();
	}

	/**
	 * Waiting the server response.
	 * @param timeout
	 * @throws InterruptedException
	 */
	public void await(long timeout) {
		try {
			latch.await(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
		}
	}
	
	/**
	 * Set result
	 * @param result
	 */
	public void setResult(TritonMessage result) {
		this.result = result;
		latch.countDown();
	}
	
	/**
	 * Get the result
	 * @return
	 */
	public TritonMessage getResult(long timeout) {
		await(timeout);
		return result;
	}
	
}
