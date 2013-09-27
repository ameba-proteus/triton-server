package com.amebame.triton.service.cassandra;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

import com.amebame.triton.client.cassandra.entity.TritonCassandraCluster;
import com.amebame.triton.client.cassandra.entity.TritonCassandraKeyspace;
import com.amebame.triton.client.cassandra.entity.TritonCassandraTable;
import com.amebame.triton.client.cassandra.method.Consistency;
import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.json.Json;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.util.ISO8601Utils;

public class CassandraConverter {
	
	private CassandraConverter() {
	}
	
	/**
	 * Convert string name to {@link TritonCassandraCluster}.
	 * @param name
	 * @return
	 */
	public static TritonCassandraCluster convertCluster(String name) {
		TritonCassandraCluster cluster = new TritonCassandraCluster();
		cluster.setName(name);
		return cluster;
	}
	
	/**
	 * Convert {@link ColumnFamilyDefinition} to {@link TritonCassandraTable}.
	 */
	public static TritonCassandraTable convertTable(TableMetadata meta) {
		TritonCassandraTable cf = new TritonCassandraTable();
		cf.setName(meta.getName());
		cf.setOptions(Json.tree(meta.getOptions()));
		return cf;
	}
	
	/**
	 * Convert list of {@link ColumnFamilyDefinition} to {@link TritonCassandraTable}.
	 * @param definition
	 * @return
	 */
	public static List<TritonCassandraTable> toTableList(Collection<TableMetadata> metaList) {
		List<TritonCassandraTable> families = new ArrayList<>(metaList.size());
		for (TableMetadata meta : metaList) {
			families.add(convertTable(meta));
		}
		return families;
	}
	
	/**
	 * Convert {@link KeyspaceDefinition} to {@link TritonCassandraKeyspace}.
	 * @param definition
	 * @return
	 */
	public static TritonCassandraKeyspace toKeyspace(KeyspaceMetadata meta) {
		TritonCassandraKeyspace keyspace = new TritonCassandraKeyspace();
		keyspace.setName(meta.getName());
		keyspace.setReplication(meta.getReplication());
		keyspace.setTables(toTableList(meta.getTables()));
		return keyspace;
	}
	
	/**
	 * Convert list of {@link KeyspaceDefinition} to {@link TritonCassandraKeyspace}.
	 * @param definitions
	 * @return
	 */
	public static List<TritonCassandraKeyspace> toKeyspaceList(List<KeyspaceMetadata> metaList, boolean skipSystem) {
		List<TritonCassandraKeyspace> keyspaces = new ArrayList<>();
		for (KeyspaceMetadata meta : metaList) {
			// skip system
			if (skipSystem && meta.getName().startsWith("system")) {
				continue;
			}
			keyspaces.add(toKeyspace(meta));
		}
		return keyspaces;
	}

	/**
	 * Convert {@link Consistency} to {@link ConsistencyLevel}.
	 * @param consistency
	 * @return
	 */
	public static ConsistencyLevel consistency(Consistency consistency) {
		if (consistency == Consistency.all) {
			return ConsistencyLevel.ALL;
		} else if (consistency == Consistency.any) {
			return ConsistencyLevel.ANY;
		} else if (consistency == Consistency.each_quorum) {
			return ConsistencyLevel.EACH_QUORUM;
		} else if (consistency == Consistency.local_quorum) {
			return ConsistencyLevel.LOCAL_QUORUM;
		} else if (consistency == Consistency.one) {
			return ConsistencyLevel.ONE;
		} else if (consistency == Consistency.quorum) {
			return ConsistencyLevel.QUORUM;
		} else if (consistency == Consistency.two) {
			return ConsistencyLevel.TWO;
		} else if (consistency == Consistency.three) {
			return ConsistencyLevel.THREE;
		} else {
			throw new TritonCassandraException(
					TritonErrors.cassandra_invalid_consistency,
					"invalid consistency level " + consistency.toString());
		}
	}
	
	public static final Object toObject(JsonNode value, DataType dataType) {
		Class<?> javaClass = dataType.asJavaClass();
		if (javaClass == String.class) {
			return value.asText();
		} else if (javaClass == Integer.class) {
			return value.asInt();
		} else if (javaClass == Long.class) {
			return value.asLong();
		} else if (javaClass == BigDecimal.class) {
			return value.decimalValue();
		} else if (javaClass == Double.class) {
			return value.asDouble();
		} else if (javaClass == Float.class) {
			return value.asDouble();
		} else if (javaClass == Boolean.class) {
			return value.asBoolean();
		} else if (javaClass == Date.class) {
			return ISO8601Utils.parse(value.asText());
		} else if (javaClass == UUID.class) {
			return UUID.fromString(value.asText());
		} else if (javaClass == ByteBuffer.class) {
			return ByteBuffer.wrap(Base64.decodeBase64(value.asText().getBytes()));
		} else {
			throw new TritonCassandraException(TritonErrors.cassandra_unsupported_datatype, javaClass + " is not supported");
		}
	}
	
	/**
	 * Convert value to element which is serializable by serializer.
	 * @param value
	 * @param serializer
	 * @return
	 */
	public static final Object toObject(String value, DataType dataType) {
		Class<?> javaClass = dataType.asJavaClass();
		if (javaClass == String.class) {
			return value;
		} else if (javaClass == Integer.class) {
			return Integer.valueOf(value);
		} else if (javaClass == Long.class) {
			return Long.valueOf(value);
		} else if (javaClass == BigDecimal.class) {
			return new BigDecimal(value);
		} else if (javaClass == Double.class) {
			return Double.valueOf(value);
		} else if (javaClass == Float.class) {
			return Float.valueOf(value);
		} else if (javaClass == Boolean.class) {
			return Boolean.valueOf(value);
		} else if (javaClass == Date.class) {
			return ISO8601Utils.parse(value);
		} else if (javaClass == UUID.class) {
			return UUID.fromString(value);
		} else if (javaClass == ByteBuffer.class) {
			return ByteBuffer.wrap(Base64.decodeBase64(value.getBytes()));
		} else {
			throw new TritonCassandraException(TritonErrors.cassandra_unsupported_datatype, javaClass + " is not supported");
		}
	}
	
	/**
	 * Convert values to element.
	 * @param values
	 * @param dataType
	 * @return
	 */
	public static final List<Object> toObjectList(List<String> values, DataType dataType) {
		return Arrays.asList(toObjectArray(values, dataType));
	}
	
	/**
	 * Convert values to element array.
	 * @param values
	 * @param dataType
	 * @return
	 */
	public static final Object[] toObjectArray(List<String> values, DataType dataType) {
		int size = values.size();
		Object[] list = new Object[values.size()];
		for (int i = 0; i < size; i++) {
			list[i] = toObject(values.get(i), dataType);
		}
		return list;
	}
	
	/**
	 * Convert json node to object list.
	 * @param node
	 * @param serializer
	 * @return
	 */
	public static final List<Object> toObjectList(JsonNode node, DataType dataType) {
		return Arrays.asList(toObjectArray(node, dataType));
	}

	/**
	 * Convert json node to object list.
	 * @param node
	 * @param serializer
	 * @return
	 */
	public static final Object[] toObjectArray(JsonNode node, DataType dataType) {
		if (node.isArray()) {
			Object[] list = new Object[node.size()];
			ArrayNode array = (ArrayNode) node;
			int size = array.size();
			for (int i = 0; i < size; i++) {
				JsonNode item = array.get(i);
				list[i] = toObject(item.asText(), dataType);
			}
			return list;
		} else {
			Object[] list = new Object[] { toObject(node.asText(), dataType) };
			return list;
		}
	}

	/**
	 * Convert map to option string.
	 * @param options
	 * @return
	 */
	public static String toCqlOptions(Map<String, Object> options) {
		if (options == null || options.isEmpty()) {
			return "{}";
		}
		StringBuilder b = new StringBuilder(128);
		for (Entry<String, Object> option : options.entrySet()) {
			if (b.length() == 0) {
				b.append('{');
			} else {
				b.append(',');
			}
			b.append('\'');
			b.append(option.getKey());
			b.append('\'');
			b.append(':');
			if (option.getValue().getClass() == String.class) {
				b.append('\'');
				b.append(option.getValue());
				b.append('\'');
			} else {
				b.append(option.getValue());
			}
		}
		b.append('}');
		return b.toString();
	}
	
	
	/**
	 * Convert object to string value.
	 * @param value
	 * @param serializer
	 * @return
	 */
	public static final String toString(Object value) {
		if (value instanceof String) {
			// return value.
			return (String) value;
		} else if (value instanceof Date) {
			// return date
			return ISO8601Utils.format((Date) value);
		} else if (value instanceof ByteBuffer) {
			ByteBuffer buffer = (ByteBuffer) value;
			byte[] bytes = new byte[buffer.limit()];
			buffer.get(bytes);
			return new String(Base64.encodeBase64(bytes));
		} else {
			return value.toString();
		}
	}
	
}
