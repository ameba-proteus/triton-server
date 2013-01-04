package com.amebame.triton.service.cassandra;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

import com.amebame.triton.exception.TritonErrors;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.partitioner.Partitioner;

/**
 * Cluster holder holds {@link Cluster} in {@link AstyanaxContext} and {@link Partitioner} implementation
 * for the cluster.
 */
public class ClusterHolder {

	private AstyanaxContext<Cluster> context;
	
	private IPartitioner<? extends Token<?>> partitioner;
	
	/**
	 * Create the cluster holder
	 * @param context
	 */
	@SuppressWarnings("unchecked")
	public ClusterHolder(AstyanaxContext<Cluster> context) {
		this.context = context;
		try {
			Cluster cluster = context.getEntity();
			this.partitioner = IPartitioner.class.cast(Class.forName(cluster.describePartitioner()).newInstance());
		} catch (ClassNotFoundException | ConnectionException | InstantiationException | IllegalAccessException e) {
			throw new TritonCassandraException(TritonErrors.cassandra_error, e);
		}
	}
	
	/**
	 * Get the cluster
	 * @return
	 */
	public Cluster getCluster() {
		return context.getEntity();
	}
	
	/**
	 * Get the context
	 * @return
	 */
	public AstyanaxContext<Cluster> getContext() {
		return context;
	}
	
	/**
	 * Get the partitioner
	 * @return
	 */
	public IPartitioner<? extends Token<?>> getPartitioner() {
		return partitioner;
	}
	
	/**
	 * Shutdown the context.
	 */
	public void shutdown() {
		this.context.shutdown();
	}

}
