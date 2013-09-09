package com.amebame.triton.service.elasticsearch;

import javax.inject.Inject;

import com.amebame.triton.server.TritonServerCleaner;
import com.amebame.triton.server.TritonServerContext;
import com.amebame.triton.service.elasticsearch.method.TritonElasticSearchMethods;

public class TritonElasticSearchSetup {
	
	@Inject private TritonServerCleaner cleaner;
	
	@Inject private TritonElasticSearchClient client;

	@Inject private TritonElasticSearchMethods methods;

	@Inject private TritonServerContext context;

	public TritonElasticSearchSetup() {
	}

	@Inject
	public void setup() {
		cleaner.add(client);
		context.addServerMethod(methods);
	}
}
