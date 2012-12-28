package com.amebame.triton.service.lock.method;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.amebame.triton.client.lock.method.LockAcquire;
import com.amebame.triton.client.lock.method.LockRelease;
import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.lock.LockContext;

public class LockMethods {
	
	private LockContext context;

	@Inject
	public LockMethods(LockContext context) {
		this.context = context;
	}
	
	@TritonMethod("lock.acquire")
	public boolean acquire(LockAcquire acquire) {
		Semaphore semaphore = context.create(acquire.getKey(), acquire.getTimeout());
		try {
			return semaphore.tryAcquire(acquire.getTimeout(), TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			return false;
		}
	}
	
	@TritonMethod("lock.release")
	public boolean release(LockRelease release) {
		Semaphore semaphore = context.get(release.getKey());
		if (semaphore != null) {
			semaphore.release();
		}
		return true;
	}

}
