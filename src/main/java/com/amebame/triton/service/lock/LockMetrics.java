/**
 * 
 */
package com.amebame.triton.service.lock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link LockMetrics} は、ロックサーバーに
 * 関する統計情報を管理します。
 * 
 * @author suguru
 *
 */
public class LockMetrics {

	private AtomicLong locks;
	
	private AtomicLong unlocks;
	
	public LockMetrics() {
		locks = new AtomicLong();
		unlocks = new AtomicLong();
	}
	
	public void increaseLocks() {
		locks.incrementAndGet();
	}
	
	public void increaseUnlocks() {
		unlocks.incrementAndGet();
	}
	
	/**
	 * ロックの回数を取得します。
	 * @return
	 */
	public long getLocks() {
		return locks.get();
	}
	
	/**
	 * 書き込みロックの回数を取得します。
	 * @return
	 */
	public long getUnlocks() {
		return unlocks.get();
	}

}
