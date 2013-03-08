package com.amebame.triton.service.lock;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Singleton;

/**
 * LockManager は、
 * ロックに関する情報を保持・管理します。
 * 
 * @author namura
 *
 */
@Singleton
public class LockManager {
	
	private ConcurrentMap<String, Lock> locks;
	
	private LockMetrics metrics;
	
	/**
	 * ロックマネージャを構成します。
	 * 
	 * @param serviceId
	 */
	public LockManager() {
		this.locks   = new ConcurrentHashMap<String, Lock>();
		this.metrics = new LockMetrics();
	}
	
	/**
	 * 統計情報を取得します。
	 * @return
	 */
	public LockMetrics getMetrics() {
		return metrics;
	}
	
	/**
	 * 名前を指定してロック情報を取得します。
	 * 
	 * @param name ロック名前
	 * @param timeout ロックタイムアウト（ミリ秒）
	 * @param create 存在しない場合に新しいロックを作成する場合は、true
	 * @return
	 */
	public void lock(LockOwner owner, String name, long timeout) {
		Lock lock = locks.get(name);
		if (lock == null) {
			lock = new Lock(name, timeout);
			Lock oldLock = locks.putIfAbsent(name, lock);
			if (oldLock != null) {
				lock = oldLock;
			}
		}
		lock.lock(owner);
	}
	
	/**
	 * ロックの解除を実行します
	 * @param owner
	 * @param name
	 */
	public void unlock(LockOwner owner, String name) {
		Lock lock = locks.get(name);
		if (lock != null) {
			lock.unlock(owner);
		}
	}
	
	/**
	 * 期限切れロックの削除
	 * 
	 * @return
	 */
	public void remove(String name) {
		locks.remove(name);
	}
	
	/**
	 * ロックの有無をチェックします。
	 * 
	 * @return ロックが1つでも存在する場合は、true を返却します。
	 */
	public boolean hasLocks() {
		return !locks.isEmpty();
	}
	
	/**
	 * すべてのロックを {@link Collection} 形式で取得します。
	 * 取得した {@link Collection} はスレッドセーフで提供されます。
	 * 
	 * @return
	 */
	public Collection<Lock> getLocks() {
		return locks.values();
	}

	/**
	 * Lock クラスは、シンプルなロックメカニズムを提供します。
	 */
	public class Lock {
		
		// ロック所有者のウエイトキュー
		private Queue<LockOwner> waiters;
		
		// 現在のロック所有者
		private LockOwner currentOwner = null;
		
		// ロックの作成時間
		private long createTime;
		
		// ロックされた時間
		private long lockTime;
		
		// ロックがタイムアウトする時間
		private long timeout;
		
		// ロック名
		private String name;

		/**
		 * Lock を作成します。
		 * 
		 * @param name ロック名称
		 * @param timeout 有効時間(ミリ秒)
		 */
		public Lock(String name, long timeout) {
			this.name = name;
			this.timeout = timeout;
			this.waiters = new LinkedList<LockOwner>();
			this.createTime = System.currentTimeMillis();
		}
		
		/**
		 * 書き込みロックの取得を試みます。
		 * 
		 * @return
		 */
		public void lock(LockOwner owner) {
			long now = System.currentTimeMillis();
			lock(owner, now, timeout);
		}
		
		/**
		 * 書き込みロックを実施します。
		 * @param owner
		 * @param lockTime
		 * @param timeout
		 * @return
		 */
		private synchronized void lock(
				LockOwner owner,
				long lockTime,
				long timeout) {
			
			metrics.increaseLocks();
			
			if (currentOwner == null) {
				// 所有者がいない場合は、カレントオーナーに設定
				currentOwner = owner;
			} else {
				// 所有者がいる場合は、キューに追加
				waiters.add(owner);
				return;
			}
			
			// ロック情報更新
			this.lockTime = lockTime;
			this.timeout = timeout;
			
			// 同期リターンを返信
			if (!owner.sendReady()) {
				// IOエラーが発生したら、即時アンロック
				unlock(owner);
			}
		}
		
		/**
		 * 書き込みロックを解除します。
		 * 
		 * @param owner
		 * @return ロックが空になった場合は true
		 */
		public synchronized void unlock(LockOwner owner) {
			
			metrics.increaseUnlocks();
			
			if (owner.getOwnerId() == currentOwner.getOwnerId()) {
				// 所有者である場合は、キューの次のオーナーを昇格
				LockOwner nextOwner = waiters.poll();
				if (nextOwner == null) {
					// 次のオーナーが存在しなければ、オーナーを空にする
					currentOwner = null;
					locks.remove(name);
					
				} else {
					
					currentOwner = nextOwner;
					
					// ロックタイムを更新
					this.lockTime = System.currentTimeMillis();
					
					if (!nextOwner.sendReady()) {
						// IOエラーが発生したら、即時アンロック
						unlock(nextOwner);
					}
				}

			} else {
				// 所有者でない場合は、キューから削除
				waiters.remove(owner);
			}
		}
		
		/**
		 * Expire lock and send fail to all waiters.
		 */
		public void expire() {
			LockOwner waiter = waiters.poll();
			while (waiter != null) {
				waiter.sendFail();
				waiter = waiters.poll();
			}
		}

		/**
		 * Get the lock name.
		 * @return
		 */
		public String getName() {
			return name;
		}
		
		/**
		 * 
		 * @return
		 */
		public long getCreateTime() {
			return createTime;
		}
		
		/**
		 * 
		 * @return
		 */
		public Date getCreateTimeAsDate() {
			return new Date(createTime);
		}
		
		/**
		 * 
		 * @return
		 */
		public long getTimeout() {
			return timeout;
		}
		
		/**
		 * 
		 * @return
		 */
		public Date getTimeoutAsDate() {
			return new Date(lockTime + timeout);
		}
		
		/**
		 * 
		 * @return
		 */
		public long getLockTime() {
			return lockTime;
		}
		
		/**
		 * 
		 * @return
		 */
		public Date getLockTimeAsDate() {
			return new Date(lockTime);
		}
		
		/**
		 * ロック完了待機を行います。
		 * 
		 * @return false 
		 */
		public synchronized boolean join(long millisec) {
			try {
				this.wait(millisec);
				return true;
			} catch (InterruptedException ex) {
				return false;
			}
		}
		
		/**
		 * 待ち状態の人数を取得します。
		 * 
		 * @return
		 */
		public int getWaiterSize() {
			return waiters.size();
		}
		
		/**
		 * 現在のオーナーを取得します。
		 * 
		 * @return
		 */
		public LockOwner getCurrentOwner() {
			return currentOwner;
		}
		
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder(256);
			builder.append("Name: ");
			builder.append(name);
			builder.append('\n');
			builder.append("Waiters: ");
			builder.append(waiters.size());
			builder.append('\n');
			builder.append("Create Time: ");
			builder.append(new Date(createTime));
			builder.append('\n');
			builder.append("Lock Time: ");
			builder.append(new Date(lockTime));
			builder.append('\n');
			builder.append("Expire Time: ");
			builder.append(new Date(lockTime + timeout));
			builder.append('\n');
			return builder.toString();
		}

	}
}
