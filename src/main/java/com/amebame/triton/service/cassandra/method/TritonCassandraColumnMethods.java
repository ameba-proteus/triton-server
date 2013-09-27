package com.amebame.triton.service.cassandra.method;

import static com.amebame.triton.service.cassandra.CassandraConverter.consistency;
import static com.amebame.triton.service.cassandra.CassandraConverter.toObject;
import static com.amebame.triton.service.cassandra.CassandraConverter.toObjectArray;
import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.token;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import javax.inject.Inject;

import org.apache.commons.codec.binary.Base64;

import com.amebame.triton.client.cassandra.method.BatchOperation;
import com.amebame.triton.client.cassandra.method.BatchOperationMode;
import com.amebame.triton.client.cassandra.method.BatchUpdate;
import com.amebame.triton.client.cassandra.method.GetColumns;
import com.amebame.triton.client.cassandra.method.RemoveColumns;
import com.amebame.triton.client.cassandra.method.SetColumns;
import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.json.Json;
import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.cassandra.CassandraColumn;
import com.amebame.triton.service.cassandra.TritonCassandraClient;
import com.amebame.triton.service.cassandra.TritonCassandraException;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.ISO8601Utils;

public class TritonCassandraColumnMethods {
	
	private static final String KEY_NAME = "id";
	private static final String COLUMN_NAME = "column";
	private static final String VALUE_NAME = "value";
	
	private TritonCassandraClient client;
	
	@Inject
	public TritonCassandraColumnMethods(TritonCassandraClient client) {
		this.client = client;
	}

	@TritonMethod("cassandra.column.set")
	public boolean setColumns(SetColumns sets) {
		// get column family
		Integer ttl = sets.getTtl();
		
		Session session = client.getSession(sets.getCluster(), sets.getKeyspace());
		
		Batch batch = batch();
		// mutate
		// set consistency level if specified
		if (sets.getConsistency() != null) {
			batch.setConsistencyLevel(consistency(sets.getConsistency()));
		}
		
		TableMetadata table = client.getTable(sets.getCluster(), sets.getKeyspace(), sets.getTable());
		_setColumns(batch, table, sets.getRows(), ttl);
		session.execute(batch);

		return true;
	}
	
	private void _setColumns(Batch batch, TableMetadata table, JsonNode rows, Integer ttl) {
		
		ColumnMetadata keyMeta = table.getColumn(KEY_NAME);
		ColumnMetadata columnMeta = table.getColumn(COLUMN_NAME);
		
		if (rows == null || rows.size() == 0) {
			return;
		}

		Iterator<Entry<String, JsonNode>> rowIterator = rows.fields();
		while (rowIterator.hasNext()) {

			Entry<String, JsonNode> row = rowIterator.next();
			String key = row.getKey();
			Object keyValue = toObject(key, keyMeta.getType());
			JsonNode columns = row.getValue();
			// prepare column mutation
			Iterator<Entry<String, JsonNode>> columnIterator = columns.fields();

			while (columnIterator.hasNext()) {
				
				Entry<String, JsonNode> column = columnIterator.next();

				String columnKey = column.getKey();
				Object columnKeyValue = toObject(columnKey, columnMeta.getType());
				String value = Json.stringify(column.getValue());
				// convert string key to type
				// get buffer from serializer
				Update update = update(table);
				// update value with JSON
				update.with(set("value", value));
				if (ttl != null) {
					update.using(ttl(ttl));
				}
				// match key and column
				update
				.where(eq(KEY_NAME, keyValue))
				.and(eq(COLUMN_NAME, columnKeyValue));
				batch.add(update);
			}
		}
	}
	
	@TritonMethod("cassandra.column.get")
	public Object getColumns(GetColumns gets) {

		TableMetadata meta = client.getTable(gets.getCluster(), gets.getKeyspace(), gets.getTable());
		Session session = client.getSession(gets.getCluster(), gets.getKeyspace());
		
		ColumnMetadata keyMeta = meta.getColumn(KEY_NAME);
		ColumnMetadata columnMeta = meta.getColumn(COLUMN_NAME);
		
		Select select = select(KEY_NAME, COLUMN_NAME, VALUE_NAME).from(gets.getTable());
	
		if (gets.hasSingleColumn()) {
			select.where(eq(COLUMN_NAME, toObject(gets.getColumns(), columnMeta.getType())));
		} else if (gets.hasColumnNames()) {
			select.where(in(COLUMN_NAME, toObjectArray(gets.getColumns(), columnMeta.getType())));
		} else if (gets.hasColumnRange()) {
			applyRangeClause(select, gets.getColumns(), columnMeta, false);
			if (gets.getColumns().has("limit")) {
				select.limit(gets.getColumns().get("limit").asInt());
			}
		}
		
		int rowLimit = 0;
		
		if (gets.isSingleKey()) {
			select.where(eq(KEY_NAME, toObject(gets.getKeys(), keyMeta.getType())));
		} else if (gets.hasKeyArray()) {
			select.where(in(KEY_NAME, toObjectArray(gets.getKeys(), keyMeta.getType())));
		} else if (gets.hasKeyRange()) {
			applyRangeClause(select, gets.getKeys(), keyMeta, true);
			if (gets.getKeys().has("limit")) {
				rowLimit = gets.getKeys().get("limit").asInt();
			}
		}
		
		if (gets.getConsitency() != null) {
			select.setConsistencyLevel(consistency(gets.getConsitency()));
		}
		
		ResultSet resultSet = session.execute(select);
		
		// Single key
		if (gets.isSingleKey()) {
			
			List<Row> rows = resultSet.all();
			if (rows.size() == 0) {
				return NullNode.instance;
			}
			
			if (gets.hasSingleColumn()) {
				return getRowJson(rows.get(0), VALUE_NAME);
			} else if (gets.hasColumnRange()) {
				List<CassandraColumn> columns = new ArrayList<>();
				for (Row row : rows) {
					columns.add(new CassandraColumn(
							getRowObject(row, COLUMN_NAME),
							getRowJson(row, VALUE_NAME)
					));
				}
				return columns;
			} else {
				ObjectNode obj = Json.object();
				for (Row row : rows) {
					String column = getRowString(row, COLUMN_NAME);
					JsonNode value = getRowJson(row, VALUE_NAME);
					obj.put(column, value);
				}
				return obj;
			}
			
		} else {

			// Multiple Key
			ObjectNode map = Json.object();
			
			for (Row row : resultSet) {

				String key = getRowString(row, KEY_NAME);
				String column = getRowString(row, COLUMN_NAME);
				JsonNode value = getRowJson(row, VALUE_NAME);

				if (gets.hasColumnRange()) {
					ArrayNode array = map.withArray(key);
					array.addObject()
					.put(COLUMN_NAME, column)
					.set(VALUE_NAME, value);
				} else {
					map
					.with(key)
					.set(column, value);
				}

				if (rowLimit != 0 && map.size() > rowLimit) {
					map.remove(key);
					rowLimit = 0;
					break;
				}
			};

			return map;
		}
	}
	
	@TritonMethod("cassandra.column.remove")
	public boolean removeColumns(RemoveColumns remove) {

		TableMetadata table = client.getTable(remove.getCluster(), remove.getKeyspace(), remove.getTable());
		
		// prepare batch for deletion
		Batch batch = batch();

		if (remove.getConsistency() != null) {
			// set consistency level if specified
			batch.setConsistencyLevel(consistency(remove.getConsistency()));
		}
		_removeColumns(batch, table, remove.getKeys(), remove.getColumns(), remove.getRows());

		Session session = client.getSession(remove.getCluster(), remove.getKeyspace());
		session.execute(batch);
		return true;
	}
	
	private void _removeColumns(Batch batch, TableMetadata table, List<String> keys, List<String> columns, JsonNode rows) {
		
		DataType keyType = table.getColumn(KEY_NAME).getType();
		DataType columnType = table.getColumn(COLUMN_NAME).getType();
		
		// iterate keys
		if (keys != null) {
			if (columns != null) {
				batch.add(delete()
						.all()
						.from(table)
						.where(in(KEY_NAME, toObjectArray(keys, keyType)))
						.and(in(COLUMN_NAME, toObjectArray(columns, columnType))));;
			} else {
				batch.add(delete()
						.all()
						.from(table)
						.where(in(KEY_NAME, toObjectArray(keys, keyType))));
			}
		}

		// iterate rows
		if (rows != null) {
			Iterator<Entry<String, JsonNode>> rowIterator = rows.fields();
			while (rowIterator.hasNext()) {
				Entry<String, JsonNode> row = rowIterator.next();

				String rowKey = row.getKey();
				// get value list
				JsonNode values = row.getValue();
				if (values != null && values.size() > 0 && values.isArray()) {
					for (JsonNode column : values) {
						batch.add(delete()
								.all()
								.from(table)
								.where(eq(KEY_NAME, toObject(rowKey, keyType)))
								.and(eq(COLUMN_NAME, toObject(column, columnType))));
					}
				}
			}
		}
	}
	
	/**
	 * Execute batch operations.
	 * @param batch
	 * @return
	 */
	@TritonMethod("cassandra.column.batch")
	public boolean batchColumns(BatchUpdate update) {
		
		// return if empty operations
		if (!update.hasOperations()) {
			return false;
		}
		
		Session session = client.getSession(update.getCluster());
		
		Batch batch = batch();
		
		if (update.getConsistency() != null) {
			batch.setConsistencyLevel(consistency(update.getConsistency()));
		}
		
		for (BatchOperation op : update.getOperations()) {
			
			TableMetadata table = client.getTable(
					update.getCluster(),
					update.getKeyspace(),
					op.getTable());
			
			if (op.getMode() == BatchOperationMode.set) {
				_setColumns(batch, table, op.getRows(), op.getTtl());
			} else if (op.getMode() == BatchOperationMode.remove) {
				_removeColumns(batch, table, op.getKeys(), op.getColumns(), op.getRows());
			}

		}
		session.execute(batch);
		return true;
	}
	
	
	
	/**
	 * 
	 * Get row json
	 * @param row
	 * @param index
	 * @return
	 */
	private JsonNode getRowJson(Row row, int index) {
		String value = row.getString(index);
		if (value == null) {
			return NullNode.instance;
		} else {
			return Json.tree(value);
		}
	}
	
	/**
	 * Force get row value as binary.
	 * @param row
	 * @param name
	 * @return
	 */
	private JsonNode getRowJson(Row row, String name) {
		int index = row.getColumnDefinitions().getIndexOf(name);
		return getRowJson(row, index);
	}
	
	private String getRowString(Row row, String name) {

		ColumnDefinitions columnDefinitions = row.getColumnDefinitions();

		int index = columnDefinitions.getIndexOf(name);
		DataType dataType = columnDefinitions.getType(index);

		if (dataType == DataType.ascii() ||
				dataType == DataType.text() ||
				dataType == DataType.varchar()) {
			return row.getString(index);
		} else if (dataType == DataType.cboolean()) {
			return String.valueOf(row.getBool(index));
		} else if (dataType == DataType.cint()) {
			return String.valueOf(row.getInt(index));
		} else if (dataType == DataType.cfloat()) {
			return String.valueOf(row.getFloat(index));
		} else if (dataType == DataType.cdouble()) {
			return String.valueOf(row.getDouble(index));
		} else if (dataType == DataType.counter() ||
				dataType == DataType.bigint()) {
			return String.valueOf(row.getLong(index));
		} else if (dataType == DataType.varint()) {
			return String.valueOf(row.getVarint(index));
		} else if (dataType == DataType.blob()) {
			return String.valueOf(row.getFloat(index));
		} else if (dataType == DataType.decimal()) {
			return row.getDecimal(index).toString();
		} else if (dataType == DataType.timestamp()) {
			return ISO8601Utils.format(row.getDate(index));
		} else if (dataType == DataType.uuid() ||
				dataType == DataType.timeuuid()) {
			return row.getUUID(index).toString();
		} else if (dataType == DataType.blob()) {
			return encodeBase64(row.getBytesUnsafe(index));
		} else {
			throw new TritonCassandraException(
					TritonErrors.cassandra_unsupported_datatype,
					"Unsupported data type " + dataType.getName().toString());
		}
	}
	
	/**
	 * Get object of the row.
	 * @param row
	 * @param name
	 * @return
	 */
	private Object getRowObject(Row row, String name) {
		ColumnDefinitions columnDefinitions = row.getColumnDefinitions();

		int index = columnDefinitions.getIndexOf(name);
		DataType dataType = columnDefinitions.getType(index);

		if (dataType == DataType.ascii() ||
				dataType == DataType.text() ||
				dataType == DataType.varchar()) {
			return row.getString(index);
		} else if (dataType == DataType.cboolean()) {
			return row.getBool(index);
		} else if (dataType == DataType.cint()) {
			return row.getInt(index);
		} else if (dataType == DataType.cfloat()) {
			return row.getFloat(index);
		} else if (dataType == DataType.cdouble()) {
			return row.getDouble(index);
		} else if (dataType == DataType.counter() ||
				dataType == DataType.bigint()) {
			return row.getLong(index);
		} else if (dataType == DataType.varint()) {
			return row.getVarint(index);
		} else if (dataType == DataType.blob()) {
			return row.getFloat(index);
		} else if (dataType == DataType.decimal()) {
			return row.getDecimal(index).toString();
		} else if (dataType == DataType.timestamp()) {
			return ISO8601Utils.format(row.getDate(index));
		} else if (dataType == DataType.uuid() ||
				dataType == DataType.timeuuid()) {
			return row.getUUID(index).toString();
		} else if (dataType == DataType.blob()) {
			return encodeBase64(row.getBytesUnsafe(index));
		} else {
			throw new TritonCassandraException(
					TritonErrors.cassandra_unsupported_datatype,
					"Unsupported data type " + dataType.getName().toString());
		}
	}
	
	private String encodeBase64(ByteBuffer buffer) {
		byte[] bytes = new byte[buffer.limit()];
		buffer.get(bytes);
		return new String(Base64.encodeBase64(bytes));
	}
	
	private Object tokenize(Object source, boolean useToken) {
		if (useToken) {
			return QueryBuilder.fcall("token", source); 
		} else {
			return source;
		}
	}
	
	private void applyRangeClause(Select select, JsonNode columns, ColumnMetadata meta, boolean useToken) {
		
		JsonNode start = columns.get("start");
		JsonNode end = columns.get("end");
		JsonNode startWith = columns.get("startWith");
		
		Select.Where where = select.where();
		
		String metaName = useToken ? token(meta.getName()) : meta.getName();
		
		if (startWith != null && !startWith.isNull()) {
			String text = startWith.asText();
			where.and(gte(metaName, tokenize(toObject(text, meta.getType()), useToken)));
			where.and(lte(metaName, tokenize(toObject(text + '\uffff', meta.getType()), useToken)));
		}
		
		if (start != null && !start.isNull()) {
			if (start.isObject()) {
				JsonNode startValue = start.get("value");
				if (start.has("exclusive") && start.get("exclusive").asBoolean()) {
					where.and(gt(metaName, tokenize(toObject(startValue, meta.getType()), useToken)));
				} else {
					where.and(gte(metaName, tokenize(toObject(startValue, meta.getType()), useToken)));
				}
			} else {
				where.and(gte(metaName, tokenize(toObject(start, meta.getType()), useToken)));
			}
		}
		
		if (end != null && !end.isNull()) {
			if (end.isObject()) {
				JsonNode endValue = end.get("value");
				if (end.has("exclusive") && start.get("exclusive").asBoolean()) {
					where.and(lt(metaName, tokenize(toObject(endValue, meta.getType()), useToken)));
				} else {
					where.and(lte(metaName, tokenize(toObject(endValue, meta.getType()), useToken)));
				}
			} else {
				where.and(lte(metaName, tokenize(toObject(end, meta.getType()), useToken)));
			}
		}
		
		if (columns.has("reverse") && columns.get("reverse").asBoolean()) {
			select.orderBy(desc(COLUMN_NAME));
		}
	}
}
