package com.amebame.triton.service.cassandra.method;

import java.util.List;

import javax.inject.Inject;

import com.amebame.triton.client.cassandra.entity.TritonCassandraKeyspace;
import com.amebame.triton.client.cassandra.method.CreateKeyspace;
import com.amebame.triton.client.cassandra.method.DescribeKeyspace;
import com.amebame.triton.client.cassandra.method.DropKeyspace;
import com.amebame.triton.client.cassandra.method.ListKeyspace;
import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.cassandra.CassandraConverter;
import com.amebame.triton.service.cassandra.TritonCassandraClient;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class TritonCassandraKeyspaceMethods {
	
	private TritonCassandraClient client;

	@Inject
	public TritonCassandraKeyspaceMethods(TritonCassandraClient client) {
		this.client = client;
	}
	
	@TritonMethod("cassandra.keyspace.list")
	public List<TritonCassandraKeyspace> listKeyspaces(ListKeyspace data) {
		Cluster cluster = client.getCluster(data.getCluster());
		return CassandraConverter.toKeyspaceList(cluster.getMetadata().getKeyspaces(), true);
	}
	
	@TritonMethod("cassandra.keyspace.detail")
	public TritonCassandraKeyspace describeKeyspace(DescribeKeyspace describe) {
		Cluster cluster = client.getCluster(describe.getCluster());
		Metadata meta = cluster.getMetadata();
		return CassandraConverter.toKeyspace(meta.getKeyspace(describe.getKeyspace()));
	}
	
	@TritonMethod("cassandra.keyspace.create")
	public boolean createKeyspace(CreateKeyspace create) {
		
		StringBuilder query = new StringBuilder("CREATE KEYSPACE ")
		.append(create.getKeyspace())
		.append(" WITH REPLICATION = ");
		
		if (create.getReplication() == null) {
			query.append("{'class':'SimpleStrategy','replication_factor':3}");
		} else {
			query.append(CassandraConverter.toCqlOptions(create.getReplication()));
		}
		
		Session session = client.getSession(create.getCluster());
		session.execute(query.toString());
		return true;
		
	}
	
	@TritonMethod("cassandra.keyspace.drop")
	public boolean dropKeyspace(DropKeyspace drop) {
		Session session = client.getSession(drop.getCluster());
		session.execute("DROP KEYSPACE " + drop.getKeyspace());
		return true;
		
	}

}
