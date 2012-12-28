package com.amebame.triton.service.memcached.method;

import java.util.Iterator;
import java.util.Map.Entry;

import javax.inject.Inject;

import com.amebame.triton.client.memcached.method.DeleteCache;
import com.amebame.triton.client.memcached.method.GetCache;
import com.amebame.triton.client.memcached.method.SetCache;
import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.memcached.TritonMemcachedClient;
import com.fasterxml.jackson.databind.JsonNode;

public class TritonMemcachedMethods {

	private TritonMemcachedClient client;
	
	@Inject
	public TritonMemcachedMethods(TritonMemcachedClient client) {
		this.client = client;
	}

	@TritonMethod("memcached.get")
	public Object get(GetCache data) {
		String cluster = data.getCluster();
		if (data.getKey() != null) {
			// get by single key
			return client.get(cluster, data.getKey(), data.getExpire());
		} else {
			// get by multiple keys
			return client.getMulti(cluster, data.getKeys());
		}
	}
	
	@TritonMethod("memcached.set")
	public boolean set(SetCache data) {
		if (data.getKey() != null) {
			// set single key-value data
			client.set(
					data.getCluster(),
					data.getKey(),
					data.getExpire(),
					data.getValue()
					);
		} else {
			// set multiple data if key is not defined
			JsonNode values = data.getValue();
			Iterator<Entry<String, JsonNode>> iterator = values.fields();
			while (iterator.hasNext()) {
				Entry<String, JsonNode> entry = iterator.next();
				client.set(
						data.getCluster(),
						entry.getKey(),
						data.getExpire(),
						entry.getValue()
				);
			}
		}
		return true;
	}
	
	@TritonMethod("memcached.delete")
	public boolean delete(DeleteCache data) {
		String cluster = data.getCluster();
		for (String key : data.getKeys()) {
			client.delete(cluster, key);
		}
		return true;
	}
}
