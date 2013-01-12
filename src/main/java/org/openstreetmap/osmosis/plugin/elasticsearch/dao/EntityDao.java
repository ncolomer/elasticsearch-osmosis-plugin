package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import java.util.logging.Logger;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.openstreetmap.osmosis.core.domain.v0_6.Bound;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;

public class EntityDao {

	private static final Logger LOG = Logger.getLogger(EntityDao.class.getName());

	private static final String NODE = "node";
	private static final String WAY = "way";

	private final String indexName;
	private final Client client;
	protected EntityMapper entityMapper;

	public EntityDao(String indexName, Client client) {
		this.client = client;
		this.indexName = indexName;
		this.entityMapper = new EntityMapper();
	}

	public String getIndexName() {
		return indexName;
	}

	public String save(Entity entity) {
		switch (entity.getType()) {
		case Node:
			return saveNode((Node) entity);
		case Way:
			return saveWay((Way) entity);
		case Relation:
			return saveRelation((Relation) entity);
		case Bound:
			return saveBound((Bound) entity);
		default:
			return null;
		}

	}

	protected String saveNode(Node node) {
		try {
			XContentBuilder xContentBuilder = entityMapper.marshallNode(node);
			return client.prepareIndex(indexName, NODE, Long.toString(node.getId()))
					.setSource(xContentBuilder)
					.execute().actionGet().getId();
		} catch (Exception e) {
			throw new DaoException("Unable to process node: " + node.toString(), e);
		}
	}

	protected String saveWay(Way way) {
		try {
			XContentBuilder xContentBuilder = entityMapper.marshallWay(way);
			return client.prepareIndex(indexName, WAY, Long.toString(way.getId()))
					.setSource(xContentBuilder)
					.execute().actionGet().getId();
		} catch (Exception e) {
			throw new DaoException("Unable to process way: " + way.toString(), e);
		}
	}

	protected String saveRelation(Relation relation) {
		LOG.warning("Save Relation is not yet supported");
		return null;
	}

	protected String saveBound(Bound bound) {
		LOG.warning("Save Bound is not yet supported");
		return null;
	}

	@SuppressWarnings("unchecked")
	public <T extends Entity> T find(long osmid, Class<T> entityClass) {
		if (entityClass == null) throw new NullPointerException("You must provide an Entity class");
		else if (entityClass.equals(Node.class)) return (T) findNode(osmid);
		else if (entityClass.equals(Way.class)) return (T) findWay(osmid);
		else if (entityClass.equals(Relation.class)) return (T) findRelation(osmid);
		else if (entityClass.equals(Bound.class)) return (T) findBound(osmid);
		else throw new IllegalArgumentException(entityClass.getSimpleName() + " is not valid");
	}

	protected Node findNode(long osmid) {
		SearchResponse searchResponse = client.prepareSearch(indexName)
				.setQuery(QueryBuilders.idsQuery(NODE).ids(Long.toString(osmid)))
				.addFields("location", "tags")
				.execute().actionGet();
		return searchResponse.getHits().getTotalHits() != 1 ? null :
				entityMapper.unmarshallNode(searchResponse.getHits().getAt(0));
	}

	protected Way findWay(long osmid) {
		SearchResponse searchResponse = client.prepareSearch(indexName)
				.setQuery(QueryBuilders.idsQuery(WAY).ids(Long.toString(osmid)))
				.addFields("tags", "nodes")
				.execute().actionGet();
		return searchResponse.getHits().getTotalHits() != 1 ? null :
				entityMapper.unmarshallWay(searchResponse.getHits().getAt(0));
	}

	protected Relation findRelation(long osmid) {
		throw new UnsupportedOperationException("Find Relation is not yet supported");
	}

	protected Bound findBound(long osmid) {
		throw new UnsupportedOperationException("Find Bound is not yet supported");
	}

}
