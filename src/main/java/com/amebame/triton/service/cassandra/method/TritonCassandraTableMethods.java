package com.amebame.triton.service.cassandra.method;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.inject.Inject;

import com.amebame.triton.client.cassandra.entity.TritonCassandraTable;
import com.amebame.triton.client.cassandra.method.CreateTable;
import com.amebame.triton.client.cassandra.method.DropTable;
import com.amebame.triton.client.cassandra.method.ListTable;
import com.amebame.triton.client.cassandra.method.TruncateTable;
import com.amebame.triton.json.Json;
import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.cassandra.TritonCassandraClient;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

public class TritonCassandraTableMethods {
	
	private TritonCassandraClient client;

	@Inject
	public TritonCassandraTableMethods(TritonCassandraClient client) {
		this.client = client;
	}

	/**
	 * Create the column family
	 * @param create
	 * @return
	 */
	@TritonMethod("cassandra.table.create")
	public boolean createTable(CreateTable create) {
		
		Session session = client.getSession(create.getCluster(), create.getKeyspace());

		StringBuilder b = new StringBuilder(128)
		.append("CREATE TABLE ")
		.append(create.getTable())
		.append(" (id ")
		.append(create.getKeyType())
		.append(", column ")
		.append(create.getColumnType())
		.append(", value text")
		.append(", PRIMARY KEY(id, column)")
		.append(") WITH comment = '")
		.append(create.getComment())
		.append('\'')
		;

		List<Object> args = new ArrayList<>();
		args.add(create.getComment());
		
		if (create.getOptions() != null) {
			for (Entry<String, Object> option : create.getOptions().entrySet()) {
				b.append(" AND ").append(option.getKey()).append(" = ");
				if (option.getValue().getClass() == String.class) {
					b.append('\'').append(option.getValue()).append('\'');
				} else {
					b.append(option.getValue());
				}
			}
		}

		session.execute(b.toString());
		return true;
		
	}
	
	/**
	 * Drop the column family
	 * @param drop
	 * @return
	 */
	@TritonMethod("cassandra.table.drop")
	public boolean dropTable(DropTable drop) {
		Session session = client.getSession(drop.getCluster(), drop.getKeyspace());
		session.execute("DROP TABLE " + drop.getTable());
		return true;

	}
	
	@TritonMethod("cassandra.table.truncate")
	public boolean truncateTable(TruncateTable truncate) {
		Session session = client.getSession(truncate.getCluster(), truncate.getKeyspace());
		session.execute("TRUNCATE " + truncate.getTable());
		return true;
	}
	
	/**
	 * List column family definitions
	 * @return
	 */
	@TritonMethod("cassandra.table.list")
	public List<TritonCassandraTable> listTable(ListTable list) {
		List<TritonCassandraTable> tables = new ArrayList<>();
		KeyspaceMetadata meta = client.getKeyspace(list.getCluster(), list.getKeyspace());
		for (TableMetadata tableMeta : meta.getTables()) {
			tables.add(convertTableMeta(tableMeta));
		}
		return tables;
	}
	
	private TritonCassandraTable convertTableMeta(TableMetadata meta) {
		TritonCassandraTable result = new TritonCassandraTable();
		result.setName(meta.getName());
		result.setOptions(Json.tree(meta.getOptions()));
		result.setKeyType(meta.getPartitionKey().get(0).getType().getName().toString());
		result.setColumnType(meta.getClusteringKey().get(0).getType().getName().toString());
		return result;
	}
	
}
