# Triton Data Access Server

Triton is the gateway for any kind of databases such as cassandra, HBase, memcached, Redis etc..

Client only need simple TCP socket which uses JSON to communicate with triton.

Triton supports middlewares such as

* Cassandra
* Memcached
* Distributed Lock (internal implementation)
* MongoDB (not yet implemented)
* Zookeeper (not yet implemented)
* HBase (not yet implemented)
* Redis (not yet implemented)

## Triton Framed Protocol

	| ------------ | --------- | ------- |--------- |
	| COMMAND_TYPE | BODY_SIZE | CALL_ID | RESERVED |
	| 2 bytes      | 4 bytes   | 4 bytes | 6 bytes  |
	| ----------------------------------------------|
	| JSON UTF8 Bytes                               |
	| ----------------------------------------------|

#### COMMAND_TYPE
* 0x0001 - COMMAND
* 0x0010 - REPLY
* 0x0011 - ERROR

## Cassandra

#### create keyspace
	cassandra.keyspace.create
	{
  	  "cluster": "clsuter",
  	  "keyspace": "keyspace",
	  "placement_strategy": "org.apache.casandra.locator.SimpleStrategy",
	  "strategy_options": {
	    "replication_factor": 3
	  }
	}

↓

	true

---
#### drop keyspace
	cassandra.keyspace.drop
	{
	  "cluster": "cluster",
	  "keyspace": "keyspace"
	}

↓

	true

---
#### list clusters
	cassandra.cluster.list
	{}

↓

	[{
	  "name": "cluster"
	}]
---
#### list keyspaces
	cassandra.keyspace.list
	{
	  "cluster": "cluster"
	}

↓

	{
	}
---
#### create column family
	cassandra.columnfamily.create
	{
	  "cluster": "cluster",
	  "keyspace": "keyspace",
	  "columnfamily": "family",
	  "
	}

↓

	true
---
#### drop column family
	cassandra.columnfamily.drop
	{
	  "cluster": "cluster",
	  "keyspace": "keyspace",
	  "columnfamily": "family",
	  "comparator": "UTF8Type",
	  "default_validation_class": "UTF8Type",
	  "key_validation_class": "UTF8Type",
	  "read_repair_chance": 0.1
	}

↓

	true
---
#### list column families
	cassandra.columnfamily.list
	{
	  "cluster": "clluster",
	  "keyspce": "keyspace"
	}

↓

	{
	}
---
#### save columns
	cassandra.column.set
	{
	  "cluster": "cluster",
	  "keyspace": "keyspace",
	  "column_family": "family",
	  "rows": {
	    "rowkey": {
	      "column1": "TEST VALUE",
	      "column2": 100,
	      "column3": {
	        "child": "can store json structure"
	      },
	    }
	  },
	  "consistency": "quorum"
	}

↓

	true
---
#### get columns

get columns with multiple names

	cassandra.column.get
	{
	  "cluster": "cluster",
	  "keyspace": "keyspace",
	  "keys": ["key1","key2"],
	  "columns": ["column1", "column2"],
	  "consistency": "one"
	}

↓

	{
	  "column1": "value1",
	  "column2": {"name1":"value1","name2":"value2"}
	}
---

get columns with range

	cassandra.column.get
	{
	  "cluster": "cluster",
	  "keyspace": "keyspace",
	  "keys": "key1",
	  "columns": {"start":"column3", "end":"column5"},
	  "consistency": "one"
	}

↓

	[
	  {"column":"column3", "value": "value3"},
	  {"column":"column4", "value": "value3"},
	  {"column":"column5", "value": "value3"}
	]
---
#### remove columns
	cassandra.columns.remove
	{
	  "cluster": "Test Cluster",
	  "keyspace": "keyspace",
	  "keys": ["key1", "key2"],
	  "columns": ["column1","column2","column3"]
	}

↓

	true
---

## Memcached

get

set

delete

## Distributed Lock

### acquire shared lock

	lock.acquire
	{
	  "key": "lock-key",
	  "timeout": 10000
	}

↓

	true

---

### release shared lock

	lock.release
	{
	  "key": "lock-key"
	}

↓

	true

---

## HBase

create table

drop table

## Zookeeper

## Redis

not yet

## MongoDB

not yet

# License

BSD