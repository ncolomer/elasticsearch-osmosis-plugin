package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import org.elasticsearch.action.delete.DeleteResponse;
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

	/**
	 * Save (index) an OSM Entity.
	 * <p>
	 * <b>Warning:</b> please note that saving {@link Relation} and
	 * {@link Bound} is not yet supported. Trying to save such {@link Entity}
	 * causes this method to throw an {@link UnsupportedOperationException}.
	 * 
	 * @param entity
	 *            the Entity object to save
	 * @return The Entity index id (actually its OSM id) as String
	 * @throws DaoException
	 *             if something was wrong during the elasticsearch request
	 */
	public String save(Entity entity) {
		if (entity == null) throw new IllegalArgumentException("You must provide an Entity");
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
			throw new DaoException("Unable to save Node: " + node.toString(), e);
		}
	}

	protected String saveWay(Way way) {
		try {
			XContentBuilder xContentBuilder = entityMapper.marshallWay(way);
			return client.prepareIndex(indexName, WAY, Long.toString(way.getId()))
					.setSource(xContentBuilder)
					.execute().actionGet().getId();
		} catch (Exception e) {
			throw new DaoException("Unable to save Way: " + way.toString(), e);
		}
	}

	protected String saveRelation(Relation relation) {
		throw new UnsupportedOperationException("Save Relation is not yet supported");
	}

	protected String saveBound(Bound bound) {
		throw new UnsupportedOperationException("Save Bound is not yet supported");
	}

	/**
	 * Find an OSM entity.
	 * <p>
	 * <b>Warning:</b> please note that finding {@link Relation} and
	 * {@link Bound} is not yet supported. Trying to find such {@link Entity}
	 * causes this method to throw an {@link UnsupportedOperationException}.
	 * 
	 * @param osmid
	 *            the OSM id that identifies the Entity
	 * @param entityClass
	 *            the class (among {@link Node}, {@link Way}, {@link Relation}
	 *            and {@link Bound}) of the Entity
	 * @return The Entity object, null if not found
	 * @throws IllegalArgumentException
	 *             if the provided entityClass is null or invalid
	 * @throws DaoException
	 *             if something was wrong during the elasticsearch request
	 */
	@SuppressWarnings("unchecked")
	public <T extends Entity> T find(long osmid, Class<T> entityClass) {
		if (entityClass == null) throw new IllegalArgumentException("You must provide an Entity class");
		else if (entityClass.equals(Node.class)) return (T) findNode(osmid);
		else if (entityClass.equals(Way.class)) return (T) findWay(osmid);
		else if (entityClass.equals(Relation.class)) return (T) findRelation(osmid);
		else if (entityClass.equals(Bound.class)) return (T) findBound(osmid);
		else throw new IllegalArgumentException(entityClass.getSimpleName() + " is not valid");
	}

	protected Node findNode(long osmid) {
		try {
			SearchResponse searchResponse = client.prepareSearch(indexName)
					.setQuery(QueryBuilders.idsQuery(NODE).ids(Long.toString(osmid)))
					.addFields("location", "tags")
					.execute().actionGet();
			return searchResponse.getHits().getTotalHits() != 1 ? null :
					entityMapper.unmarshallNode(searchResponse.getHits().getAt(0));
		} catch (Exception e) {
			throw new DaoException("Unable to find Node [" + osmid + "]", e);
		}
	}

	protected Way findWay(long osmid) {
		try {
			SearchResponse searchResponse = client.prepareSearch(indexName)
					.setQuery(QueryBuilders.idsQuery(WAY).ids(Long.toString(osmid)))
					.addFields("tags", "nodes")
					.execute().actionGet();
			return searchResponse.getHits().getTotalHits() != 1 ? null :
					entityMapper.unmarshallWay(searchResponse.getHits().getAt(0));
		} catch (Exception e) {
			throw new DaoException("Unable to find Way [" + osmid + "]", e);
		}
	}

	protected Relation findRelation(long osmid) {
		throw new UnsupportedOperationException("Find Relation is not yet supported");
	}

	protected Bound findBound(long osmid) {
		throw new UnsupportedOperationException("Find Bound is not yet supported");
	}

	/**
	 * Delete an OSM entity.
	 * <p>
	 * <b>Warning:</b> please note that deleting {@link Relation} and
	 * {@link Bound} is not yet supported. Trying to delete such {@link Entity}
	 * causes this method to throw an {@link UnsupportedOperationException}.
	 * 
	 * @param osmid
	 *            the OSM id that identifies the Entity
	 * @param entityClass
	 *            the class (among {@link Node}, {@link Way}, {@link Relation}
	 *            and {@link Bound}) of the Entity
	 * @return True if the Entity was deleted, false otherwise (i.e. the Entity
	 *         was not found)
	 * @throws IllegalArgumentException
	 *             if the provided entityClass is null or invalid
	 * @throws DaoException
	 *             if something was wrong during the elasticsearch request
	 */
	public <T extends Entity> boolean delete(long osmid, Class<T> entityClass) {
		if (entityClass == null) throw new IllegalArgumentException("You must provide an Entity class");
		else if (entityClass.equals(Node.class)) return deleteEntity(osmid, NODE);
		else if (entityClass.equals(Way.class)) return deleteEntity(osmid, WAY);
		else if (entityClass.equals(Relation.class)) throw new UnsupportedOperationException("Delete Relation is not yet supported");
		else if (entityClass.equals(Bound.class)) throw new UnsupportedOperationException("Delete Bound is not yet supported");
		else throw new IllegalArgumentException(entityClass.getSimpleName() + " is not valid");
	}

	protected boolean deleteEntity(long osmid, String type) {
		try {
			DeleteResponse response = client.prepareDelete(indexName, type, Long.toString(osmid))
					.execute().actionGet();
			return !response.notFound();
		} catch (Exception e) {
			throw new DaoException("Unable to delete " + type + " [" + osmid + "]", e);
		}
	}

}
