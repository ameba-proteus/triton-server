/*
 * Copyright 2008 Suguru Namura.
 *
 * This source is licensed under a
 * Creative Commons Attribution-Noncommercial 3.0
 * United States License.
 * 
 * See detail at http://creativecommons.org/licenses/by-nc/3.0/us/
 */
package com.amebame.triton.service.lock;

import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import com.amebame.triton.protocol.TritonMessage;

/**
 * Lock Owner
 */
public class LockOwner implements Comparable<LockOwner> {
	
	private static final AtomicInteger OWNER_ID_COUNTER = new AtomicInteger();
	
	// Channel
	private Channel channel;

	// OwnerID
	private int ownerId;
	
	// CallID
	private int callId;

	// Time which lock created
	private long createTime;

	public LockOwner(Channel channel, int callId) {
		this(channel, callId, 0);
	}
	
	public LockOwner(Channel channel, int callId, int ownerId) {
		this.channel = channel;
		if (ownerId == 0) {
			ownerId = OWNER_ID_COUNTER.incrementAndGet();
			if (ownerId > 0xffffff) {
				OWNER_ID_COUNTER.set(0);
			}
		}
		this.ownerId = ownerId;
		this.callId = callId;
		this.createTime = System.currentTimeMillis();
	}
	
	public long getLockTime() {
		return createTime;
	}
	
	public Channel getChannel() {
		return channel;
	}
	
	public int getCallId() {
		return callId;
	}
	
	public int getOwnerId() {
		return ownerId;
	}
	
	/**
	 * Send ready to the owner client.
	 * @return
	 */
	public boolean sendReady() {
		TritonMessage message = new TritonMessage(TritonMessage.REPLY, callId, ownerId);
		if (!channel.isOpen()) {
			return false;
		}
		ChannelFuture future = channel.write(message);
		try {
			if (future.await(1000L)) {
				return future.isSuccess();
			}
		} catch (InterruptedException e) {
		}
		return false;
	}
	
	/**
	 * Send fail to the owner client.
	 * @return
	 */
	public boolean sendFail() {
		TritonMessage message = new TritonMessage(TritonMessage.REPLY, callId, -1);
		ChannelFuture future = channel.write(message);
		try {
			if (future.await(1000L)) {
				return future.isSuccess();
			}
		} catch (InterruptedException e) {
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return (int)(callId ^ (callId >>> 32));
	}
	
	@Override
	public int compareTo(LockOwner o) {
		long thisTime = createTime;
		long otherTime = o.createTime;
		return (thisTime < otherTime) ? -1 : (thisTime == otherTime) ? 0 : -1;
	}
	
	@Override
	public String toString() {
		return new StringBuilder(64)
			.append("LockOwner-")
			.append(channel.getId())
			.toString();
	}
}
