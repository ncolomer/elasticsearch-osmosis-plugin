package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.openstreetmap.osmosis.core.domain.v0_6.Bound;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;

public class EntityDao {

	private static final Logger LOG = Logger.getLogger(EntityDao.class.getName());

	protected String indexName;

	protected IndexAdminService indexAdminService;

	protected EntityMapper entityMapper;

	public EntityDao(String indexName, IndexAdminService indexAdminService) {
		this.indexAdminService = indexAdminService;
		this.indexName = indexName;
		this.entityMapper = new EntityMapper();
	}

	public String getIndexName() {
		return indexName;
	}

	public void save(Entity entity) {
		switch (entity.getType()) {
		case Node:
			saveNode((Node) entity);
			break;
		case Way:
			saveWay((Way) entity);
			break;
		case Relation:
			saveRelation((Relation) entity);
			break;
		case Bound:
			saveBound((Bound) entity);
			break;
		}
	}

	protected void saveNode(Node node) {
		try {
			XContentBuilder xContentBuilder = entityMapper.marshallNode(node);
			indexAdminService.index(indexName, "node", node.getId(), xContentBuilder);
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Unable to process node: " + e.getMessage(), e);
		}
	}

	protected void saveWay(Way way) {
		try {
			XContentBuilder xContentBuilder = entityMapper.marshallWay(way);
			indexAdminService.index(indexName, "way", way.getId(), xContentBuilder);
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Unable to process way: " + e.getMessage(), e);
		}
	}

	protected void saveRelation(Relation relation) {
		LOG.warning(String.format("Unable to process relation with osmid [%d]:" +
				" processing of Relations has not been implemented yet", relation.getId()));
	}

	protected void saveBound(Bound bound) {
		LOG.warning(String.format("Unable to process bound with osmid [%d]:" +
				" processing of Bounds has not been implemented yet", bound.getId()));
	}

	@SuppressWarnings("unchecked")
	public <T extends Entity> T find(long osmid, Class<T> entityClass) {
		if (entityClass == null) throw new NullPointerException();
		else if (entityClass.equals(Node.class)) return (T) findNode(osmid);
		else if (entityClass.equals(Way.class)) return (T) findWay(osmid);
		else if (entityClass.equals(Relation.class)) return (T) findRelation(osmid);
		else if (entityClass.equals(Bound.class)) return (T) findBound(osmid);
		else throw new IllegalArgumentException(entityClass.getSimpleName() + " is not valid");
	}

	protected Node findNode(long osmid) {
		SearchRequestBuilder searchRequest = findNodeQuery(osmid);
		SearchResponse searchResponse = indexAdminService.execute(searchRequest);
		return searchResponse.getHits().getTotalHits() != 1 ? null :
				entityMapper.unmarshallNode(searchResponse.getHits().getAt(0));
	}

	protected SearchRequestBuilder findNodeQuery(long osmid) {
		return indexAdminService.getClient().prepareSearch("osm")
				.setQuery(QueryBuilders.idsQuery("node").ids(Long.toString(osmid)))
				.addFields("location", "tags");
	}

	protected Way findWay(long osmid) {
		SearchRequestBuilder searchRequest = findWayQuery(osmid);
		SearchResponse searchResponse = indexAdminService.execute(searchRequest);
		return searchResponse.getHits().getTotalHits() != 1 ? null :
				entityMapper.unmarshallWay(searchResponse.getHits().getAt(0));
	}

	protected SearchRequestBuilder findWayQuery(long osmid) {
		return indexAdminService.getClient().prepareSearch("osm")
				.setQuery(QueryBuilders.idsQuery("way").ids(Long.toString(osmid)))
				.addFields("tags", "nodes");
	}

	protected Relation findRelation(long osmid) {
		throw new UnsupportedOperationException("Find Relation is not yet supported");
	}

	protected Bound findBound(long osmid) {
		throw new UnsupportedOperationException("Find Bound is not yet supported");
	}

}
