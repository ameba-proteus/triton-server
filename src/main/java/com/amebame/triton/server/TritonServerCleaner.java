package com.amebame.triton.server;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Singleton;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class TritonServerCleaner {
	
	private static final Logger log = LogManager.getLogger(TritonServerCleaner.class);
	
	private List<TritonCleaner> cleaners;

	public TritonServerCleaner() {
		this.cleaners = new ArrayList<TritonCleaner>();
	}

	public void add(TritonCleaner cleaner) {
		this.cleaners.add(cleaner);
	}
	
	public void add(final Runnable runnable) {
		this.cleaners.add(new TritonCleaner() {
			@Override
			public void clean() {
				runnable.run();
			}
		});
	}
	
	public void clean() {
		for (TritonCleaner cleaner : cleaners) {
			try {
				cleaner.clean();
			} catch (Exception e) {
				log.error("failed to cleanup", e);
			}
		}
	}
}
