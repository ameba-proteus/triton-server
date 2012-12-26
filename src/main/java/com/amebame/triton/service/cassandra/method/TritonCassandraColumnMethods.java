package com.amebame.triton.service.cassandra.method;

import javax.inject.Inject;

import com.amebame.triton.client.cassandra.method.GetColumns;
import com.amebame.triton.client.cassandra.method.RemoveColumns;
import com.amebame.triton.client.cassandra.method.SetColumns;
import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.cassandra.TritonCassandraClient;

public class TritonCassandraColumnMethods {
	
	private TritonCassandraClient client;

	@Inject
	public TritonCassandraColumnMethods(TritonCassandraClient client) {
		this.client = client;
	}

	@TritonMethod("cassandra.column.set")
	public boolean setColumns(SetColumns sets) {
		client.setColumns(sets);
		return true;
	}
	
	@TritonMethod("cassandra.column.get")
	public void getColumns(GetColumns gets) {
		client.getColumns(gets);
	}
	
	@TritonMethod("cassandra.column.remove")
	public boolean removeColumns(RemoveColumns removes) {
		client.removeColumns(removes);
		return true;
	}
}
