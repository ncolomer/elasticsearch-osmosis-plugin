package org.openstreetmap.osmosis.plugin.elasticsearch.builder.highway;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.existsFilter;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.plugin.elasticsearch.builder.AbstractIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Endpoint;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Parameters;

public class HighwayIndexBuilder extends AbstractIndexBuilder {

	private static final Logger LOG = Logger.getLogger(HighwayIndexBuilder.class.getName());

	public HighwayIndexBuilder(Endpoint endpoint, Parameters params) {
		super(endpoint, params);
	}

	@Override
	public String getSpecializedIndexSuffix() {
		return "highway";
	}

	@Override
	public void buildIndex() {

		int bulkSize = Integer.valueOf(getParameters().getProperty(getSpecializedIndexSuffix() + ".bulk.size"));
		int scrollTimeout = 60000;

		SearchResponse scrollResp = getClient().prepareSearch(getEntityIndexName())
				.setSearchType(SearchType.SCAN).setScroll(new TimeValue(scrollTimeout)).setSize(bulkSize)
				.setTypes(EntityDao.WAY)
				.setQuery(matchAllQuery())
				.setFilter(existsFilter("tags.highway"))
				.addFields("tags", "nodes")
				.execute().actionGet();

		while (true) {
			BulkRequestBuilder bulkRequest = getClient().prepareBulk();
			scrollResp = getClient().prepareSearchScroll(scrollResp.getScrollId())
					.setScroll(new TimeValue(scrollTimeout))
					.execute().actionGet();
			if (scrollResp.hits().hits().length == 0) break;
			for (SearchHit way : scrollResp.hits()) {
				try {
					XContentBuilder source = buildWay(way);
					IndexRequestBuilder request = getClient()
							.prepareIndex(getSpecializedIndexName(), EntityDao.WAY, way.getId())
							.setSource(source);
					bulkRequest.add(request);
				} catch (Exception e) {
					LOG.severe("Unable to prepare Way [" + way.getId() + "]: " + e.getMessage());
				}
			}
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
				for (BulkItemResponse r : bulkResponse.items()) {
					if (r.failed()) {
						LOG.severe("Unable to index Way [" + r.getId() + "]: " + r.getFailureMessage());
					}
				}
			}
		}

	}

	private XContentBuilder buildWay(SearchHit way) throws IOException {
		XContentBuilder wayBuilder = jsonBuilder().startObject();
		// Add Way tags
		wayBuilder.field("tags", way.field("tags").value());
		// Build Way line
		wayBuilder.startObject("line");
		wayBuilder.field("type", "linestring");
		wayBuilder.startArray("coordinates");
		List<Object> nodeIds = way.field("nodes").values();
		for (double[] object : buildLine(nodeIds)) {
			wayBuilder.startArray();
			wayBuilder.value(object[0]);
			wayBuilder.value(object[1]);
			wayBuilder.endArray();
		}
		wayBuilder.endArray();
		wayBuilder.endObject();
		wayBuilder.endObject();
		return wayBuilder;
	}

	private double[][] buildLine(List<Object> nodeIds) throws IOException {
		// Build ids array
		long[] nodeIdsLongs = new long[nodeIds.size()];
		for (int i = 0; i < nodeIds.size(); i++) {
			nodeIdsLongs[i] = ((Long) nodeIds.get(i)).longValue();
		}
		// Fetch nodes
		List<Node> nodes = getEntityDao().findAll(Node.class, nodeIdsLongs);
		// Build line
		double[][] line = new double[nodeIds.size()][2];
		for (int i = 0; i < nodes.size(); i++) {
			// Format as [lon, lat] to conform with GeoJSON.
			line[i] = new double[] { nodes.get(i).getLongitude(), nodes.get(i).getLatitude() };
		}
		return line;
	}

}