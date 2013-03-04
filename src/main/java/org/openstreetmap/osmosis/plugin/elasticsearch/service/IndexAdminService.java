package org.openstreetmap.osmosis.plugin.elasticsearch.service;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class IndexAdminService {

	protected final Client client;

	public IndexAdminService(Client client) {
		this.client = client;
	}

	public void createIndex(String indexName, String indexConfig) {
		// Delete previous existing index
		if (indexExists(indexName)) deleteIndex(indexName);
		// Create the new index
		client.admin().indices().prepareCreate(indexName)
				.setSource(indexConfig)
				.execute().actionGet();
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
		client.admin().indices().prepareRefresh(indices)
				.execute().actionGet();
	}

	public Client getClient() {
		return client;
	}

}
