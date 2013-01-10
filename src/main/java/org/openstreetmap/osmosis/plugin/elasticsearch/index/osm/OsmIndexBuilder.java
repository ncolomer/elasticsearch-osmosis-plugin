package org.openstreetmap.osmosis.plugin.elasticsearch.index.osm;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.index.IndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;

public class OsmIndexBuilder implements IndexBuilder {

	@Override
	public String getIndexName() {
		return null;
	}

	@Override
	public Map<String, XContentBuilder> getIndexMapping() {
		try {
			Map<String, XContentBuilder> mapping = new HashMap<String, XContentBuilder>();
			XContentBuilder nodeMapping = jsonBuilder()
					.startObject().startObject("node").startObject("properties")
					.startObject("location").field("type", "geo_point").endObject()
					.endObject().endObject().endObject();
			mapping.put("node", nodeMapping);
			XContentBuilder wayMapping = jsonBuilder()
					.startObject().startObject("way").startObject("properties")
					.startObject("nodes").field("type", "long").field("store", "yes").endObject()
					.endObject().endObject().endObject();
			mapping.put("way", wayMapping);
			return mapping;
		} catch (IOException e) {
			throw new IllegalStateException("Unable to create mapping", e);
		}
	}

	@Override
	public void buildIndex(IndexAdminService indexAdminService) {}

}
