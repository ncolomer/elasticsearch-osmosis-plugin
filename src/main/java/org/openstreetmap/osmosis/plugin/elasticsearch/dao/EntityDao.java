package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.openstreetmap.osmosis.core.domain.v0_6.Bound;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexService;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.OsmUtils;

public class EntityDao {

	private static final Logger LOG = Logger.getLogger(EntityDao.class.getName());

	protected final IndexService indexService;

	protected final String indexName;

	public EntityDao(IndexService indexService, String indexName) {
		this.indexService = indexService;
		this.indexName = indexName;
		if (!indexService.indexExists(indexName)) throw new IllegalStateException(
				"The index " + indexName + " does not exist");

	}

	public void save(Node node) {
		if (node == null) throw new IllegalArgumentException("Provided node is null");
		try {
			XContentBuilder sourceBuilder = jsonBuilder()
					.startObject()
					.field("id", node.getId())
					.field("location", new Object[] { node.getLongitude(), node.getLatitude() })
					.field("tags", OsmUtils.getTags(node))
					.endObject();
			indexService.index(indexName, node.getType(), node.getId(), sourceBuilder);
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Unable to process node: " + e.getMessage(), e);
		}
	}

	public void save(Way way) {
		if (way == null) throw new IllegalArgumentException("Provided way is null");
		try {
			XContentBuilder sourceBuilder = jsonBuilder()
					.startObject()
					.field("id", way.getId())
					.field("tags", OsmUtils.getTags(way))
					.field("nodes", OsmUtils.getNodes(way))
					.endObject();
			indexService.index(indexName, way.getType(), way.getId(), sourceBuilder);
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Unable to process way: " + e.getMessage(), e);
		}
	}

	public void save(Relation relation) {
		LOG.warning(String.format("Unable to process relation with osmid %d:" +
				" processing of Relations has not been implemented yet", relation.getId()));
	}

	public void save(Bound bound) {
		LOG.warning(String.format("Unable to process bound with osmid %d:" +
				" processing of Bounds has not been implemented yet", bound.getId()));
	}

	public void release() {
		indexService.refresh(indexName);
		indexService.close();
	}

}
