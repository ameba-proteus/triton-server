package com.amebame.triton.service.lock;

import java.util.Iterator;

import javax.inject.Inject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amebame.triton.service.lock.LockManager.Lock;

/**
 * Cleaning lock object which are expired.
 * @author namura
 */
public class LockClearner implements Runnable {
	
	private static final Logger log = LogManager.getLogger(LockClearner.class);

	private LockManager manager;
	
	@Inject
	public LockClearner(LockManager manager) {
		this.manager = manager;
	}

	@Override
	public void run() {

		// ロックが保持されていなければ、処理をパス
		if (!manager.hasLocks()) {
			return;
		}

		long now = System.currentTimeMillis();
		
		// ロック一覧を取得し、期限切れチェック
		Iterator<Lock> iterator = manager.getLocks().iterator();
		while (iterator.hasNext()) {
			Lock lock = iterator.next();
			if (now > lock.getLockTime() + lock.getTimeout()) {
				// クリーニング数確認のため、ログレベルをwarnに
				log.info("Lock has been expired [{}]", lock.getName());
				try {
					lock.expire();
				} catch (Exception e) {
					log.error("Failed to expire lock [{}]", lock.getName(), e);
				}
				iterator.remove();
			}
		}
	}
}
