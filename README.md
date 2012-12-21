# Triton Data Access Server

Triton is the gateway for any kind of databases such as cassandra, HBase, memcached, Redis etc..

Client only need simple TCP socket which uses JSON to communicate with triton.

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
  	  "cluster": "Test Clsuter",
  	  "keyspace": "keyspace",
	  "placement_strategy": "org.apache.casandra.locator.SimpleStrategy",
	  "strategy_options": {
	    "replication_factor": 3
	  }
	}

---
	true

#### drop keyspace
	cassandra.keyspace.drop
	{
	  "cluster": "Test Cluster",
	  "keyspace": "keyspace"
	}
---
	true

#### list clusters
	cassandra.cluster.list
	{}
---
	[{
	  "name": "Test Cluster"
	}]

#### list keyspaces
	cassandra.keyspace.list
	{
	  "cluster": "Test Cluster"
	}
---
	{
	}

#### create column family
	cassandra.columnfamily.create
	{
	  "cluster": "Test Cluster",
	  "keyspace": "keyspace",
	  "columnfamily": "family",
	  "
	}
---
	true

#### drop column family
	cassandra.columnfamily.drop
	{
	  "cluster": "Test Cluster",
	  "keyspace": "keyspace",
	  "columnfamily": "family",
	  "comparator": "UTF8Type",
	  "default_validation_class": "UTF8Type",
	  "key_validation_class": "UTF8Type",
	  "read_repair_chance": 0.1
	}
---
	true

#### list column families
	cassandra.columnfamily.list
	{
	  "cluster": "Test Cluster",
	  "keyspce": "keyspace"
	}
---
	{
	}

#### save columns
	cassandra.column.set
	{
	  "cluster": "Test Cluster",
	  "keyspace": "keyspace",
	  "columns": {
	    "key": {
	      "column1": "TEST VALUE",
	      "column2": 100,
	      "column3": {
	        "child": "can store json structure"
	      },
	    }
	  },
	  "consistency": "quorum"
	}
---
	true

#### get columns
	cassandra.column.get
	{
	  "cluster": "Test Cluster",
	  "keyspace": "keyspace",
	  "keys": ["key1","key2"],
	  "columns": ["column1", "column2"],
	  "consistency": "one"
	}
---
	{
	}

#### remove columns
	cassandra.columns.remove
	{
	  "cluster": "Test Cluster",
	  "keyspace": "keyspace",
	  "keys": ["key1", "key2"],
	  "columns": ["column1","column2","column3"]
	}
---
	true

## HBase

create table

drop table

## Memcached

get

set

delete

incr/decr

flush

## Redis

not yet

## MongoDB

not yet