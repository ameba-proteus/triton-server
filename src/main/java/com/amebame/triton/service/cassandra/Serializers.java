package com.amebame.triton.service.cassandra;

import java.math.BigInteger;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.amebame.triton.exception.TritonErrors;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.AsciiSerializer;
import com.netflix.astyanax.serializers.BigIntegerSerializer;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.FloatSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

public class Serializers {
	
	private static final Map<String, Serializer<?>> SERIALIZERS = build();

	public Serializers() {
	}
	
	private static final Map<String, Serializer<?>> build() {
		/*
		 * BytesType, AsciiType, UTF8Type, IntegerType, LongType,
		 * UUIDType, TimeUUIDType, DateType, BooleanType, FloatType,
		 * DoubleType, DecimalType, CounterColumnType
		 * @param defaultValidationClass
		 */
		Map<String, Serializer<?>> map = new HashMap<String, Serializer<?>>();
		map.put("BytesType", BytesArraySerializer.get());
		map.put("AsciiType", AsciiSerializer.get());
		map.put("UTF8Type", StringSerializer.get());
		map.put("IntegerTYpe", IntegerSerializer.get());
		map.put("LongType", LongSerializer.get());
		map.put("UUIDType", UUIDSerializer.get());
		map.put("TimeUUIDType", TimeUUIDSerializer.get());
		map.put("DateType", DateSerializer.get());
		map.put("BooleanType", BooleanSerializer.get());
		map.put("FloatType", FloatSerializer.get());
		map.put("DoubleType", DoubleSerializer.get());
		map.put("DecimalType", BigIntegerSerializer.get());
		map.put("CounterColumnType", LongSerializer.get());
		return map;
	}
	
	/**
	 * Get serializer by type name
	 * @param columnType
	 * @return
	 */
	public static Serializer<?> get(String columnType) {
		Serializer<?> serializer = SERIALIZERS.get(columnType);
		if (serializer == null) {
			serializer = BytesArraySerializer.get();
		}
		return serializer;
	}
	
	@SuppressWarnings("unchecked")
	public static <T> Serializer<T> get(Class<T> clazz) {
		if (clazz == byte[].class) {
			return (Serializer<T>) BytesArraySerializer.get();
		} else if (clazz == String.class) {
			return (Serializer<T>) StringSerializer.get();
		} else if (clazz == Integer.class) {
			return (Serializer<T>) IntegerSerializer.get();
		} else if (clazz == Long.class) {
			return (Serializer<T>) LongSerializer.get();
		} else if (clazz == UUID.class) {
			return (Serializer<T>) UUIDSerializer.get();
		} else if (clazz == Date.class) {
			return (Serializer<T>) DateSerializer.get();
		} else if (clazz == Boolean.class) {
			return (Serializer<T>) BooleanSerializer.get();
		} else if (clazz == Float.class) {
			return (Serializer<T>) FloatSerializer.get();
		} else if (clazz == Double.class) {
			return (Serializer<T>) DoubleSerializer.get();
		} else if (clazz == BigInteger.class) {
			return (Serializer<T>) BigIntegerSerializer.get();
		} else {
			throw new TritonCassandraException(
					TritonErrors.cassandra_error,
					"no serializer for " + clazz.getSimpleName());
		}
	}

}
