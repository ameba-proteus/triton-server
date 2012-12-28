package com.amebame.triton.service.lock;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

import javax.inject.Singleton;

import org.jboss.netty.util.internal.ConcurrentHashMap;

@Singleton
public class LockContext implements Runnable {
	
	private ConcurrentMap<String, LockHolder> locks;

	public LockContext() {
		locks = new ConcurrentHashMap<>();
	}
	
	/**
	 * Create {@link Semaphore} associated with the key.
	 * Semaphore will be created if not exist.
	 * @param key
	 * @return
	 */
	public Semaphore create(String key, int timeout) {
		LockHolder holder = locks.get(key);
		long now = System.currentTimeMillis();
		long expire = now + timeout;
		if (holder == null) {
			// create new lock
			holder = new LockHolder(expire);
			// put lock and check old value
			LockHolder old = locks.putIfAbsent(key, holder);
			if (old != null) {
				// swap holder if old is exist which is inserted concurrently
				holder = old;
			}
		}
		// update expire time to recent expire
		if (holder.expire < expire) {
			holder.expire = expire;
		}
		return holder.semaphore;
	}
	
	/**
	 * Get {@link Semaphore} associated with the key.
	 * @param key
	 * @return
	 */
	public Semaphore get(String key) {
		LockHolder holder = locks.get(key);
		if (holder == null) {
			return null;
		} else {
			return holder.semaphore;
		}
	}
	
	/**
	 * Clean expired locks.
	 */
	public void clean() {
		long now = System.currentTimeMillis();
		Iterator<LockHolder> iterator = locks.values().iterator();
		while (iterator.hasNext()) {
			LockHolder holder = iterator.next();
			// remove if holder reaches expire time.
			if (now > holder.expire) {
				iterator.remove();
			}
		}
	}
	
	@Override
	public void run() {
		clean();
	}

	/**
	 * LockHolder to hold {@link Lock} and expire time
	 */
	private static class LockHolder {
		private Semaphore semaphore;
		private long expire;
		private LockHolder(long expire) {
			this.semaphore = new Semaphore(1);
			this.expire = expire;
		}
	}
}
