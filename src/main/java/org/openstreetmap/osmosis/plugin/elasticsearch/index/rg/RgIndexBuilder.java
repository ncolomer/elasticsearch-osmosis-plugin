package org.openstreetmap.osmosis.plugin.elasticsearch.index.rg;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.index.IndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexService;

public class RgIndexBuilder implements IndexBuilder {

	@Override
	public String getIndexName() {
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
	public void buildIndex(IndexService indexService) {
		// TODO: to implement
		/*
		 * SearchResponse response =
		 * indexService.getClient().prepareSearch("osm").setTypes("way")
		 * .setQuery(matchAllQuery()) .setFilter(existsFilter("highway"))
		 * .setNoFields().execute().actionGet(); // We get all ways for
		 * (SearchHit hit : response.getHits()) { SearchHit way =
		 * indexService.getClient().prepareSearch("osm").setTypes("way")
		 * .setQuery(QueryBuilders.idsQuery("way").ids(hit.getId()))
		 * .execute().actionGet().getHits().getAt(0); // For this way, we
		 * retrieve all wayNodes List<Long> wayNodes =
		 * way.field("nodes").<List<Long>> getValue(); for (Long wayNode :
		 * wayNodes) { } }
		 */
	}

}
