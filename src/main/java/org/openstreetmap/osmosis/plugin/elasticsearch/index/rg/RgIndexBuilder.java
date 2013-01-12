package org.openstreetmap.osmosis.plugin.elasticsearch.index.rg;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.index.AbstractIndexBuilder;

public class RgIndexBuilder extends AbstractIndexBuilder {

	public RgIndexBuilder(Client client, EntityDao entityDao, String indexName) {
		super(client, entityDao, indexName);
	}

	@Override
	public String getSpecializedIndexSuffix() {
		return "rg";
	}

	@Override
	public Map<String, XContentBuilder> getIndexMapping() {
		try {
			Map<String, XContentBuilder> mapping = new HashMap<String, XContentBuilder>();
			XContentBuilder xContentBuilder = jsonBuilder()
					.startObject()
					.startObject("way").startObject("properties")
					.startObject("nodes").startObject("properties")
					.startObject("location").field("type", "geo_point").endObject()
					.endObject().endObject()
					.endObject().endObject()
					.endObject();
			mapping.put("way", xContentBuilder);
			return mapping;
		} catch (IOException e) {
			throw new IllegalStateException("Unable to create mapping", e);
		}
	}

	@Override
	public void buildIndex() {
		// TODO To implement
	}

}
