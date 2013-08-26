package com.amebame.triton.service.lock.method;

import io.netty.channel.Channel;

import javax.inject.Inject;

import com.amebame.triton.client.lock.method.LockAcquire;
import com.amebame.triton.client.lock.method.LockRelease;
import com.amebame.triton.protocol.TritonMessage;
import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.lock.LockManager;
import com.amebame.triton.service.lock.LockOwner;

public class LockMethods {
	
	private LockManager manager;

	@Inject
	public LockMethods(LockManager manager) {
		this.manager = manager;
	}
	
	@TritonMethod(value="lock.acquire", async=true)
	public void acquire(Channel channel, TritonMessage message, LockAcquire acquire) {
		LockOwner owner = new LockOwner(channel, message.getCallId());
		manager.lock(owner, acquire.getKey(), acquire.getTimeout());
	}
	
	@TritonMethod("lock.release")
	public boolean release(Channel channel, TritonMessage message, LockRelease release) {
		LockOwner owner = new LockOwner(channel, message.getCallId(), release.getOwnerId());
		manager.unlock(owner, release.getKey());
		return true;
	}

}
