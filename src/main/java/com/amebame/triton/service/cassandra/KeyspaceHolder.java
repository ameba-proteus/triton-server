package com.amebame.triton.service.cassandra;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;

/**
 * KeyspaceHolder holds {@link AstyanaxContext}
 */
public class KeyspaceHolder {
	
	// astyanax context
	private AstyanaxContext<Keyspace> context;
	
	/**
	 * 
	 * @param context
	 */
	public KeyspaceHolder(AstyanaxContext<Keyspace> context) {
		this.context = context;
	}
	
	/**
	 * Get the context
	 * @return
	 */
	public AstyanaxContext<Keyspace> getContext() {
		return context;
	}
	
	/**
	 * Get the keyspace
	 * @return
	 */
	public Keyspace getKeyspace() {
		return context.getEntity();
	}
	
	/**
	 * Shutdown the context
	 */
	public void shutdown() {
		context.shutdown();
	}
}
