package org.openstreetmap.osmosis.plugin.elasticsearch.service;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class IndexAdminService {

	private final Client client;

	public IndexAdminService(Client client) {
		this.client = client;
	}

	public void createIndex(String name, int shards, int replicas, String mappings) {
		try {
			// Delete previous existing index
			if (indexExists(name)) deleteIndex(name);
			// Build index configuration
			String configuration = XContentFactory.jsonBuilder().startObject()
					.startObject("settings").field("number_of_shards", shards)
					.field("number_of_replicas", replicas).endObject()
					.rawField("mappings", mappings.getBytes())
					.endObject().string();
			// Create the new index
			client.admin().indices().prepareCreate(name)
					.setSource(configuration)
					.execute().actionGet();
		} catch (Exception e) {
			throw new RuntimeException("Unable to create index " + name, e);
		}
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

}
