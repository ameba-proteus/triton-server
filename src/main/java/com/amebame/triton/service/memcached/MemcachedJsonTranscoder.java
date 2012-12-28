package com.amebame.triton.service.memcached;

import net.rubyeye.xmemcached.transcoders.CachedData;
import net.rubyeye.xmemcached.transcoders.PrimitiveTypeTranscoder;

import com.amebame.triton.json.Json;
import com.fasterxml.jackson.databind.JsonNode;

public class MemcachedJsonTranscoder extends PrimitiveTypeTranscoder<JsonNode> {

	public MemcachedJsonTranscoder() {
	}

	@Override
	public CachedData encode(JsonNode o) {
		CachedData data = new CachedData(0, Json.bytes(o));
		return data;
	}

	@Override
	public JsonNode decode(CachedData d) {
		return Json.tree(d.getData());
	}

}
