package com.amebame.triton.service.elasticsearch.method;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.Facets;

import com.amebame.triton.client.elasticsearch.method.DeleteDocument;
import com.amebame.triton.client.elasticsearch.method.GetDocument;
import com.amebame.triton.client.elasticsearch.method.GetDocumentResult;
import com.amebame.triton.client.elasticsearch.method.IndexDocument;
import com.amebame.triton.client.elasticsearch.method.IndexDocumentResult;
import com.amebame.triton.client.elasticsearch.method.MultiGetDocument;
import com.amebame.triton.client.elasticsearch.method.MultiGetDocumentResult;
import com.amebame.triton.client.elasticsearch.method.SearchDocument;
import com.amebame.triton.client.elasticsearch.method.SearchDocumentHit;
import com.amebame.triton.client.elasticsearch.method.SearchDocumentResult;
import com.amebame.triton.client.elasticsearch.method.UpdateDocument;
import com.amebame.triton.client.elasticsearch.method.UpdateDocumentResult;
import com.amebame.triton.config.TritonElasticSearchConfiguration;
import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.json.Json;
import com.amebame.triton.server.TritonMethod;
import com.amebame.triton.service.elasticsearch.TritonElasticSearchClient;
import com.amebame.triton.service.elasticsearch.TritonElasticSearchException;
import com.fasterxml.jackson.databind.JsonNode;

public class TritonElasticSearchMethods {
	
	private TritonElasticSearchClient clients;
	
	private TritonElasticSearchConfiguration config;

	@Inject
	public TritonElasticSearchMethods(
			TritonElasticSearchClient clients,
			TritonElasticSearchConfiguration config) {
		this.clients = clients;
		this.config = config;
	}
	
	private TimeValue timeout() {
		return TimeValue.timeValueMillis(config.getTimeout());
	}
	
	/**
	 * Index the document.
	 * @param index
	 * @return
	 */
	@TritonMethod("elasticsearch.index")
	public IndexDocumentResult index(IndexDocument index) {
		
		Client client = clients.getClient(index.getCluster());
		IndexRequestBuilder builder = client.prepareIndex(index.getIndex(), index.getType(), index.getId());
		
		// set TTL if exists
		if (index.getTtl() > 0L) {
			builder.setTTL(index.getTtl());
		}
		
		builder
		.setSource(Json.bytes(index.getSource()))
		.setRefresh(index.isRefresh())
		.setRouting(index.getRouting())
		;
		
		IndexResponse response = builder.get(timeout());
		return new IndexDocumentResult(response.getId(), response.getVersion());
		
	}
	
	/**
	 * Get the document by id
	 * @param get
	 * @return
	 */
	@TritonMethod("elasticsearch.get")
	public GetDocumentResult get(GetDocument get) {

		Client client = clients.getClient(get.getCluster());
		GetResponse response = client
				.prepareGet(get.getIndex(), get.getType(), get.getId())
				.setRouting(get.getRouting())
				.get(timeout());

		if (response.isExists()) {
			
			GetDocumentResult result = new GetDocumentResult();
			result.setId(response.getId());
			result.setVersion(response.getVersion());
			result.setSource(response.getSource());
			return result;

		} else {
			
			// return null if not exist
			return null;
		}
	}
	
	/**
	 * Get multiple documents by ids.
	 * @param multiGet
	 * @return
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@TritonMethod("elasticsearch.multiget")
	public MultiGetDocumentResult multiget(MultiGetDocument multiGet) {
		
		Client client = clients.getClient(multiGet.getCluster());
		MultiGetResponse response = client.prepareMultiGet()
				.add(multiGet.getIndex(), multiGet.getType(), multiGet.getIds())
				.get(timeout());
		
		MultiGetDocumentResult result = new MultiGetDocumentResult();

		for (MultiGetItemResponse item : response.getResponses()) {
			if (item.isFailed()) {
				result.addFail(item.getId(), item.getFailure().getMessage());
			} else {
				GetDocumentResult itemResult = new GetDocumentResult();
				GetResponse itemResponse = item.getResponse();
				itemResult.setId(item.getId());
				itemResult.setVersion(itemResponse.getVersion());
				itemResult.setSource(itemResponse.getSource());
				result.addItem(itemResult);
			}
		}
		return result;

	}
	
	/**
	 * Search documents by query.
	 * @param search
	 */
	@TritonMethod("elasticsearch.search")
	public SearchDocumentResult search(SearchDocument search) {
		
		Client client = clients.getClient(search.getCluster());
		
		SearchResponse response = null;

		if (search.hasScrollId()) {
			response = searchScroll(client, search.getScrollId());
		} else {
			response = searchQuery(client, search);
		}

		SearchHits hits = response.getHits();
		String scrollId = response.getScrollId();
		Facets facets = response.getFacets();

		SearchDocumentResult result = new SearchDocumentResult();
		
		// Convert facets field.
		if (facets != null) {
			try {
				XContent content = XContentFactory.xContent(XContentType.JSON);
				XContentBuilder builder = XContentBuilder.builder(content);
				for (Facet facet : facets) {
					if (facet instanceof ToXContent) {
						((ToXContent) facet).toXContent(builder, ToXContent.EMPTY_PARAMS);
					}
				}
				BytesReference bytesRef = builder.bytes();
				JsonNode facetNode = Json.tree(bytesRef.toBytes());
				result.setFacets(facetNode);

			} catch (IOException e) {
				throw new TritonElasticSearchException(TritonErrors.elasticsearch_error, e.getMessage(), e);
			}
		}
		
		// Convert hit field
		if (hits != null) {
			result.setTotal(hits.totalHits());

			for (SearchHit hit : hits) {
				SearchDocumentHit hitItem = new SearchDocumentHit();
				hitItem.setId(hit.getId());
				hitItem.setIndex(hit.getIndex());
				hitItem.setType(hit.getType());
				hitItem.setScore(hit.getScore());
				JsonNode source = Json.tree(hit.getSourceRef().toBytes());
				hitItem.setSource(source);
				result.addHit(hitItem);
			}
		}
		
		result.setScrollId(scrollId);
		result.setTook(response.getTookInMillis());
		
		return result;
	}
	
	/**
	 * Search by scroll ID
	 * @param client
	 * @param scrollId
	 * @return
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private SearchResponse searchScroll(Client client, String scrollId) {
		return client.prepareSearchScroll(scrollId).get(timeout());
	}
	
	/**
	 * Search by document
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private SearchResponse searchQuery(Client client, SearchDocument search) {

		SearchRequestBuilder builder = client.prepareSearch(search.getIndices());
		
		builder
		.setTypes(search.getTypes())
		.setFrom(search.getFrom())
		.setSize(search.getSize())
		;
		
		if (search.hasRoutings()) {
			builder.setRouting(search.getRoutings());
		}
		
		if (search.hasSearchType()) {
			builder.setSearchType(search.getSearchType());
		}

		if (search.hasQuery()) {
			builder.setQuery(Json.bytes(search.getQuery()));
		}
		
		if (search.hasFilter()) {
			builder.setFilter(Json.bytes(search.getFilter()));
		}

		if (search.hasFacets()) {
			builder.setFacets(Json.bytes(search.getFacets()));
		}
		
		return builder.get(timeout());
	}
	
	@TritonMethod("elasticsearch.update")
	public UpdateDocumentResult update(UpdateDocument update) {

		Client client = clients.getClient(update.getCluster());
		
		UpdateRequestBuilder builder = client.prepareUpdate(update.getIndex(), update.getType(), update.getId());
		
		if (update.hasDoc()) {
			builder.setDoc(Json.stringify(update.getDoc()));
		}
		if (update.hasUpsert()) {
			builder.setUpsert(Json.stringify(update.getUpsert()));
		}
		if (update.hasScript()) {
			builder.setScript(update.getScript());
		}
		if (update.hasParams()) {
			builder.setScriptParams(update.getParams());
		}
		
		builder
		.setConsistencyLevel(WriteConsistencyLevel.QUORUM)
		.setRouting(update.getRouting())
		.setRefresh(update.isRefresh())
		.setDocAsUpsert(update.isDocAsUpsert())
		;
		
		UpdateResponse response = builder.get(timeout());
		GetResult getResult = response.getGetResult();

		UpdateDocumentResult result = new UpdateDocumentResult();
		result.setId(response.getId());
		result.setVersion(response.getVersion());

		if (getResult != null && getResult.source() != null) {
			result.setSource(Json.tree(getResult.source()));
		}
		return result;
	}
	
	@TritonMethod("elasticsearch.delete")
	public boolean delete(DeleteDocument delete) {

		Client client = clients.getClient(delete.getCluster());
		
		client
		.prepareDelete()
		.setIndex(delete.getIndex())
		.setType(delete.getType())
		.setId(delete.getId())
		.setRefresh(delete.isRefresh())
		.get(timeout())
		;
		
		return true;
	}

}
