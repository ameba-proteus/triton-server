package com.amebame.triton.service.cassandra.entity;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.model.ColumnFamily;

public class TritonColumnFamily {
	
	private ColumnFamilyDefinition definition;
	
	private ColumnFamily<?, ?> columnFamily;
	
	private SerializerPackage serializerPackage;
	
	public TritonColumnFamily(
			ColumnFamilyDefinition definition,
			ColumnFamily<?, ?> columnFamily,
			SerializerPackage serializerPackage) {
		this.definition = definition;
		this.columnFamily = columnFamily;
		this.serializerPackage = serializerPackage;
	}
	
	public ColumnFamilyDefinition getDefinition() {
		return definition;
	}
	
	public ColumnFamily<?, ?> getColumnFamily() {
		return columnFamily;
	}
	
	public SerializerPackage getSerializerPackage() {
		return serializerPackage;
	}

	public Serializer<?> getKeySerializer() {
		return serializerPackage.getKeySerializer();
	}
	
	public Serializer<?> getColumnNameSerializer() {
		return serializerPackage.getColumnNameSerializer();
	}
	
	public Serializer<?> getDefaultValueSerializer() {
		return serializerPackage.getDefaultValueSerializer();
	}
}
