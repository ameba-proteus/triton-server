package com.amebame.triton.service.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amebame.triton.client.cassandra.entity.TritonCassandraCluster;
import com.amebame.triton.client.cassandra.entity.TritonCassandraColumnFamily;
import com.amebame.triton.client.cassandra.entity.TritonCassandraKeyspace;
import com.amebame.triton.client.cassandra.method.Consistency;
import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.json.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.AsciiSerializer;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.FloatSerializer;
import com.netflix.astyanax.serializers.Int32Serializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

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
	 * Convert {@link ColumnFamilyDefinition} to {@link TritonCassandraColumnFamily}.
	 */
	public static TritonCassandraColumnFamily convertColumnFamily(ColumnFamilyDefinition definition) {
		TritonCassandraColumnFamily cf = new TritonCassandraColumnFamily();
		cf.setName(definition.getName());
		cf.setCaching(definition.getCaching());
		cf.setComment(definition.getComment());
		cf.setCompactionStrategy(definition.getCompactionStrategy());
		cf.setCompactionStrategyOptions(definition.getCompactionStrategyOptions());
		cf.setComparatorType(definition.getComparatorType());
		cf.setCompressionOptions(definition.getCompressionOptions());
		cf.setDefaultValidationClass(definition.getDefaultValidationClass());
		cf.setGcGraceSeconds(definition.getGcGraceSeconds());
		cf.setKeyValidationClass(definition.getKeyValidationClass());
		return  cf;
	}
	
	/**
	 * Convert list of {@link ColumnFamilyDefinition} to {@link TritonCassandraColumnFamily}.
	 * @param definition
	 * @return
	 */
	public static List<TritonCassandraColumnFamily> toColumnFamilyList(List<ColumnFamilyDefinition> definitions) {
		List<TritonCassandraColumnFamily> families = new ArrayList<>(definitions.size());
		for (ColumnFamilyDefinition definition : definitions) {
			families.add(convertColumnFamily(definition));
		}
		return families;
	}
	
	/**
	 * Convert {@link KeyspaceDefinition} to {@link TritonCassandraKeyspace}.
	 * @param definition
	 * @return
	 */
	public static TritonCassandraKeyspace toKeyspace(KeyspaceDefinition definition) {
		TritonCassandraKeyspace keyspace = new TritonCassandraKeyspace();
		keyspace.setName(definition.getName());
		keyspace.setStrategyClass(definition.getStrategyClass());
		keyspace.setStrategyOptions(definition.getStrategyOptions());
		keyspace.setColumnFamilies(toColumnFamilyList(definition.getColumnFamilyList()));
		return keyspace;
	}
	
	/**
	 * Convert list of {@link KeyspaceDefinition} to {@link TritonCassandraKeyspace}.
	 * @param definitions
	 * @return
	 */
	public static List<TritonCassandraKeyspace> toKeyspaceList(List<KeyspaceDefinition> definitions, boolean skipSystem) {
		List<TritonCassandraKeyspace> keyspaces = new ArrayList<>();
		for (KeyspaceDefinition definition : definitions) {
			// skip system
			if (skipSystem && definition.getName().startsWith("system")) {
				continue;
			}
			keyspaces.add(toKeyspace(definition));
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
			return ConsistencyLevel.CL_ALL;
		} else if (consistency == Consistency.any) {
			return ConsistencyLevel.CL_ANY;
		} else if (consistency == Consistency.each_quorum) {
			return ConsistencyLevel.CL_EACH_QUORUM;
		} else if (consistency == Consistency.local_quorum) {
			return ConsistencyLevel.CL_LOCAL_QUORUM;
		} else if (consistency == Consistency.one) {
			return ConsistencyLevel.CL_ONE;
		} else if (consistency == Consistency.quorum) {
			return ConsistencyLevel.CL_QUORUM;
		} else if (consistency == Consistency.two) {
			return ConsistencyLevel.CL_TWO;
		} else if (consistency == Consistency.three) {
			return ConsistencyLevel.CL_THREE;
		} else {
			throw new TritonCassandraException(
					TritonErrors.cassandra_invalid_consistency,
					"invalid consistency level " + consistency.toString());
		}
	}
	
	/**
	 * Convert value to element which is serializable by serializer.
	 * @param value
	 * @param serializer
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static final <E> E toObject(String value, Serializer<E> serializer) {
		Class<?> serializerClass = serializer.getClass();
		if (serializerClass == StringSerializer.class) {
			return (E) value;
		} else if (serializerClass == IntegerSerializer.class || serializerClass == Int32Serializer.class) {
			return (E) Integer.valueOf(value);
		} else if (serializerClass == LongSerializer.class) {
			return (E) Long.valueOf(value);
		} else if (serializerClass == DoubleSerializer.class) {
			return (E) Double.valueOf(value);
		} else if (serializerClass == FloatSerializer.class) {
			return (E) Float.valueOf(value);
		} else if (serializerClass == BooleanSerializer.class) {
			return (E) Boolean.valueOf(value);
		} else {
			return serializer.fromByteBuffer(serializer.fromString(value));
		}
	}
	
	/**
	 * Convert values to element which is serializable by serializer.
	 * @param values
	 * @param serializer
	 * @return
	 */
	public static final <E> List<E> toObjectList(List<String> values, Serializer<E> serializer) {
		List<E> list = new ArrayList<>(values.size());
		for (String value : values) {
			list.add(toObject(value, serializer));
		}
		return list;
	}
	
	/**
	 * Convert json node to object list.
	 * @param node
	 * @param serializer
	 * @return
	 */
	public static final <E> List<E> toObjectList(JsonNode node, Serializer<E> serializer) {
		List<E> list = new ArrayList<>();
		if (node.isArray()) {
			ArrayNode array = (ArrayNode) node;
			int size = array.size();
			for (int i = 0; i < size; i++) {
				JsonNode item = array.get(i);
				list.add(toObject(item.asText(), serializer));
			}
		} else {
			list.add(toObject(node.asText(), serializer));
		}
		return list;
	}
	
	/**
	 * Convert object to string value.
	 * @param value
	 * @param serializer
	 * @return
	 */
	public static final <E> String toString(E value, Serializer<E> serializer) {
		Class<?> serializerClass = serializer.getClass();
		if (serializerClass == StringSerializer.class) {
			// return value.
			return (String) value;
		} else if (serializerClass == IntegerSerializer.class ||
				serializerClass == Int32Serializer.class ||
				serializerClass == LongSerializer.class ||
				serializerClass == DoubleSerializer.class ||
				serializerClass == FloatSerializer.class ||
				serializerClass == BooleanSerializer.class) {
			// return normal string for primitive values.
			return value.toString();
		} else {
			// return string value through serializing.
			return serializer.getString(serializer.toByteBuffer(value));
		}
	}
	
	/**
	 * Convet json node to object which can serialize by the serializer.
	 * @param value
	 * @param serializer
	 * @return
	 */
	public static final ByteBuffer toValueBuffer(JsonNode value, Serializer<?> serializer) {
		Class<?> serializerClass = serializer.getClass();
		if (serializerClass == BytesArraySerializer.class
				|| serializerClass == ByteBufferSerializer.class
				|| serializerClass == StringSerializer.class
				|| serializerClass == AsciiSerializer.class) {
			// write as JSON object
			return Json.buffer(value);
		} else {
			// write as specific format from text
			return serializer.fromString(value.asText());
		}
	}
	
	/**
	 * Convert binary value to JsonNode with {@link Serializer}
	 * @param bytes
	 * @param serializer
	 * @return
	 */
	public static final JsonNode toValueNode(byte[] bytes, Serializer<?> serializer) {
		Class<?> serializerClass = serializer.getClass();
		if (serializerClass == BytesArraySerializer.class
				|| serializerClass == ByteBufferSerializer.class
				|| serializerClass == StringSerializer.class
				|| serializerClass == AsciiSerializer.class) {
			// make json tree from stored binary
			return Json.tree(bytes);
		} else {
			// Make json node from serialized value
			return Json.tree(serializer.fromBytes(bytes));
		}
	}
	
	/**
	 * Convert {@link ColumnList} to List of {@link CassandraColumn}
	 * @param columns
	 * @param columnSerializer
	 * @return
	 */
	public static final <C> List<CassandraColumn<C>> toCassandraColumnList(ColumnList<C> columns, Serializer<C> columnSerializer) {
		List<CassandraColumn<C>> list = new ArrayList<>(columns.size());
		for (Column<C> column : columns) {
			list.add(new CassandraColumn<>(
					column.getName(),
					CassandraConverter.toValueNode(column.getByteArrayValue(), columnSerializer)));
		}
		return list;
	}
	
	/**
	 * Convert {@link ColumnList} to Map of {@link String} and {@link JsonNode}
	 * @param columns
	 * @param columnSerializer
	 * @param valueSerializer
	 * @return
	 */
	public static final <C,V> Map<String, JsonNode> toCassandraColumnMap(ColumnList<C> columns, Serializer<C> columnSerializer, Serializer<V> valueSerializer) {
		Map<String, JsonNode> map = new HashMap<>(columns.size());
		for (Column<C> column : columns) {
			map.put(
					CassandraConverter.toString(column.getName(), columnSerializer),
					CassandraConverter.toValueNode(column.getByteArrayValue(), valueSerializer)
					);
		}
		return map;
	}
	
}
