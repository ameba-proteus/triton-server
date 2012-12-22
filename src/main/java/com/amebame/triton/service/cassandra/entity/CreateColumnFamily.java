package com.amebame.triton.service.cassandra.entity;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(Include.NON_NULL)
public class CreateColumnFamily {
	
	private String cluster;
	
	private String keyspace;
	
	@JsonProperty("column_family")
	private String columnFamily;
	
	private String caching;
	
	private String comparator = "BytesType";
	
	private String comment;
	
	private String compactionStrategy;
	
	private Map<String, String> compactionStrategyOptions;
	
	private String defaultValidationClass = "BytesType";
	
	@JsonProperty("dclocal_read_repair_chance")
	private Double dclocalReadRepairChance;
	
	@JsonProperty("gc_grace_seconds")
	private Integer GcGraceSeconds;
	
	@JsonProperty("key_validation_class")
	private String keyValidationClass = "BytesType";
	
	public CreateColumnFamily() {
	}

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public String getCaching() {
		return caching;
	}

	/**
	 * all, keys_only, rows_only, none
	 * @param caching
	 */
	public void setCaching(String caching) {
		this.caching = caching;
	}

	public String getComparator() {
		return comparator;
	}

	/**
	 * BytesType, AsciiType, UTF8Type, IntegerType, LongType,
	 * UUIDType, TimeUUIDType, DateType, BooleanType, FloatType,
	 * DoubleType, DecimalType, CounterColumnType
	 * @param comparator
	 */
	public void setComparator(String comparator) {
		this.comparator = comparator;
	}

	public String getComment() {
		return comment;
	}

	/**
	 * Any text
	 * @param comment
	 */
	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getCompactionStrategy() {
		return compactionStrategy;
	}

	/**
	 * SizeTieredCompactionStrategy, LeveledCompactionStrategy
	 * @param comparator
	 */
	public void setCompactionStrategy(String compactionStrategy) {
		this.compactionStrategy = compactionStrategy;
	}

	public Map<String, String> getCompactionStrategyOptions() {
		return compactionStrategyOptions;
	}

	/**
	 * sstable_size_in_mb
	 * @param compactionStrategyOptions
	 */
	public void setCompactionStrategyOptions(
			Map<String, String> compactionStrategyOptions) {
		this.compactionStrategyOptions = compactionStrategyOptions;
	}
	
	public String getDefaultValidationClass() {
		return defaultValidationClass;
	}

	/**
	 * BytesType, AsciiType, UTF8Type, IntegerType, LongType,
	 * UUIDType, TimeUUIDType, DateType, BooleanType, FloatType,
	 * DoubleType, DecimalType, CounterColumnType
	 * @param defaultValidationClass
	 */
	public void setDefaultValidationClass(String defaultValidationClass) {
		this.defaultValidationClass = defaultValidationClass;
	}

	public Double getDclocalReadRepairChance() {
		return dclocalReadRepairChance;
	}

	/**
	 * Default 0.0
	 * @param dclocalReadRepairChance
	 */
	public void setDclocalReadRepairChance(Double dclocalReadRepairChance) {
		this.dclocalReadRepairChance = dclocalReadRepairChance;
	}

	public Integer getGcGraceSeconds() {
		return GcGraceSeconds;
	}
	
	/**
	 * Default 864000 (10days)
	 * @param gcGraceSeconds
	 */
	public void setGcGraceSeconds(Integer gcGraceSeconds) {
		GcGraceSeconds = gcGraceSeconds;
	}

	public String getKeyValidationClass() {
		return keyValidationClass;
	}

	/**
	 * BytesType, AsciiType, UTF8Type, IntegerType, LongType,
	 * UUIDType, TimeUUIDType, DateType, BooleanType, FloatType,
	 * DoubleType, DecimalType, CounterColumnType
	 * @param keyValidationClass
	 */
	public void setKeyValidationClass(String keyValidationClass) {
		this.keyValidationClass = keyValidationClass;
	}

}
