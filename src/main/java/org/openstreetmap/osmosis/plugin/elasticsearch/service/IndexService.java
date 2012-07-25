package org.openstreetmap.osmosis.plugin.elasticsearch.service;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;

public class IndexService {

	protected final Client client;

	public IndexService(Client client) {
		this.client = client;
	}

	public boolean indexExists(String... indices) {
		return client.admin().indices().prepareExists(indices)
				.execute().actionGet().exists();
	}

	public void createIndex(String indexName) {
		try {
			// Delete previous existing index
			if (indexExists(indexName)) {
				client.admin().indices().prepareDelete(indexName).execute().actionGet();
			}
			// Build mapping for geo fields
			XContentBuilder source = jsonBuilder()
					.startObject().startObject(EntityType.Node.name().toLowerCase()).startObject("properties")
					.startObject("location").field("type", "geo_point").endObject()
					.endObject().endObject().endObject();
			// Create the new index
			client.admin().indices().prepareCreate(indexName)
					.addMapping(EntityType.Node.name().toLowerCase(), source)
					.execute()
					.actionGet();
		} catch (Exception e) {
			throw new IllegalStateException("Unable to recreate index " + indexName, e);
		}
	}

	public void index(String index, EntityType type, long id, XContentBuilder sourceBuilder) {
		client.prepareIndex(index, type.name().toLowerCase(), Long.toString(id))
				.setSource(sourceBuilder)
				.execute()
				.actionGet();
	}

	public void refresh(String... indices) {
		client.admin().indices().refresh(Requests.refreshRequest(indices)).actionGet();
	}

	public void close() {
		client.close();
	}

}
