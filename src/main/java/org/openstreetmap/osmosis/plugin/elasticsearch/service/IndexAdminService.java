package org.openstreetmap.osmosis.plugin.elasticsearch.service;

import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class IndexAdminService {

	protected final Client client;

	public IndexAdminService(Client client) {
		this.client = client;
	}

	public void createIndex(String indexName, Map<String, XContentBuilder> map) {
		// Delete previous existing index
		if (indexExists(indexName)) deleteIndex(indexName);
		// Create the new index
		CreateIndexRequestBuilder builder = client.admin().indices().prepareCreate(indexName);
		for (String key : map.keySet())
			builder.addMapping(key, map.get(key));
		builder.execute().actionGet();
	}

	public boolean indexExists(String... indices) {
		return client.admin().indices().prepareExists(indices)
				.execute().actionGet().exists();
	}

	public void index(String index, String type, long id, XContentBuilder sourceBuilder) {
		client.prepareIndex(index, type, Long.toString(id))
				.setSource(sourceBuilder)
				.execute().actionGet();
	}

	public void deleteIndex(String... indices) {
		client.admin().indices().prepareDelete(indices)
				.execute().actionGet();
	}

	public void deleteDocument(String indexName, String type, String id) {
		client.prepareDelete()
				.setIndex(indexName)
				.setType(type)
				.setId(id)
				.execute().actionGet();
	}

	public void refresh(String... indices) {
		client.admin().indices().refresh(Requests.refreshRequest(indices)).actionGet();
	}

	public Client getClient() {
		return client;
	}

}
