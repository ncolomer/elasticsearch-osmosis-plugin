package org.openstreetmap.osmosis.plugin.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.logging.Logger;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.ModelUtils;

public class ElasticSearchWriterTask implements Sink {

	private static final Logger LOG = Logger.getLogger(ElasticSearchWriterTask.class.getName());

	private int boundProcessedCounter = 0;
	private int nodeProcessedCounter = 0;
	private int wayProcessedCounter = 0;
	private int relationProcessedCounter = 0;

	private Client client;

	public ElasticSearchWriterTask(Client client) {
		this.client = client;
	}

	@Override
	public void release() {
		client.close();
	}

	@Override
	public void process(EntityContainer entityContainer) {
		// SearchRequestBuilder srb = client.prepareSearch(index);
		// srb.setQuery(QueryBuilders.matchAllQuery());
		// srb.setFilter(FilterBuilders.geoDistanceRangeFilter("filter1").lat(1234).lon(4321).geoDistance(GeoDistance.PLANE)
		// ..... );
		Entity entity = entityContainer.getEntity();
		switch (entity.getType()) {
		case Bound:
			// Bound bound = (Bound) entity;
			this.boundProcessedCounter++;
			break;

		case Node:
			onNode((Node) entity);
			this.nodeProcessedCounter++;
			break;

		case Way:
			onWay((Way) entity);
			this.wayProcessedCounter++;
			break;
		case Relation:
			// Relation relation = (Relation) entity;
			this.relationProcessedCounter++;
			break;
		}
	}

	@Override
	public void complete() {
		LOG.info("Completed!\n" +
				"total processed bounds: ..... " + boundProcessedCounter + "\n" +
				"total processed nodes: ...... " + nodeProcessedCounter + "\n" +
				"total processed ways: ....... " + wayProcessedCounter + "\n" +
				"total processed relations: .. " + relationProcessedCounter);
		float consumedMemoryMb = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
				/ (float) Math.pow(1024, 2);
		LOG.info(String.format("Estimated memory consumption: %.2f MB ", consumedMemoryMb));
	}

	protected void onWay(Way way) {
		try {
			XContentBuilder sourceBuilder = jsonBuilder()
					.startObject()
					.field("id", way.getId())
					.field("tags", ModelUtils.getTagsFromWay(way))
					.field("nodes", ModelUtils.getNodesFromWay(way))
					.endObject();
			LOG.info(sourceBuilder.string());
			index("osm", "way", way.getId(), sourceBuilder);
		} catch (Exception e) {
			LOG.severe("Unable to process way: " + e.getMessage());
		}
	}

	protected void onNode(Node node) {
		try {
			XContentBuilder sourceBuilder = jsonBuilder()
					.startObject()
					.field("id", node.getId())
					.field("location", ModelUtils.getLonLatFromNode(node))
					.endObject();
			LOG.info(sourceBuilder.string());
			index("osm", "node", node.getId(), sourceBuilder);
		} catch (Exception e) {
			LOG.severe("Unable to process way: " + e.getMessage());
		}
	}

	protected void index(String index, String type, Long id, XContentBuilder sourceBuilder) {
		client.prepareIndex(index, type, Long.toString(id))
				.setSource(sourceBuilder)
				.execute()
				.actionGet();
	}

}
