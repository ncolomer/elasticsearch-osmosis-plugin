package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.openstreetmap.osmosis.core.domain.v0_6.Bound;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;

public class EntityDao {

	private static final Logger LOG = Logger.getLogger(EntityDao.class.getName());

	public static final String NODE = "node";
	public static final String WAY = "way";

	private final String indexName;
	private final Client client;
	protected EntityMapper entityMapper;

	public EntityDao(String indexName, Client client) {
		this.client = client;
		this.indexName = indexName;
		this.entityMapper = new EntityMapper();
	}

	/**
	 * Save (index) an OSM Entity.
	 * <p>
	 * <b>Warning:</b> please note that saving {@link Relation} and
	 * {@link Bound} is not yet supported. Trying to save such {@link Entity}
	 * causes this method to throw an {@link DaoException}.
	 * 
	 * @param entity
	 *            the Entity object to save
	 * @throws DaoException
	 *             if something was wrong during the save process
	 */
	public void save(Entity entity) {
		try {
			buildIndexRequest(entity).execute().actionGet();
		} catch (Exception e) {
			throw new DaoException("Unable to save Entity [" + entity + "]", e);
		}
	}

	/**
	 * Save (index) all OSM Entities using a bulk request.
	 * <p>
	 * All errors caught during the bulk request building or entities indexing
	 * are handled silently, i.e. logged and ignored.
	 * <p>
	 * <b>Warning:</b> please note that saving {@link Relation} and
	 * {@link Bound} is not yet supported. Trying to save such {@link Entity}
	 * causes this method to ignore it silently.
	 * 
	 * @param entities
	 *            the List of Entity objects to save
	 */
	public void saveAll(List<Entity> entities) {
		if (entities == null || entities.isEmpty()) return;
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (Entity entity : entities) {
			try {
				bulkRequest.add(buildIndexRequest(entity));
			} catch (Exception exception) {
				LOG.warning("Unable to add Entity [" + entity + "] to bulk request: " + exception.getMessage());
			}
		}
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			for (BulkItemResponse response : bulkResponse.items()) {
				if (response.failed()) {
					LOG.warning("Unable to index Entity [index=" + response.getIndex() +
							", type=" + response.getType() + ", id=" + response.getId() + "]: " +
							response.getFailureMessage());
				}
			}
		}
	}

	protected IndexRequestBuilder buildIndexRequest(Entity entity) throws Exception {
		if (entity == null) throw new IllegalArgumentException("You must provide a non-null Entity");
		switch (entity.getType()) {
		case Node:
			return client.prepareIndex(indexName, EntityDao.NODE, Long.toString(entity.getId()))
					.setSource(entityMapper.marshallNode((Node) entity));
		case Way:
			return client.prepareIndex(indexName, EntityDao.WAY, Long.toString(entity.getId()))
					.setSource(entityMapper.marshallWay((Way) entity));
		case Relation:
			throw new UnsupportedOperationException("Save Relation is not yet supported");
		case Bound:
			throw new UnsupportedOperationException("Save Bound is not yet supported");
		default:
			throw new IllegalArgumentException("Unknown Entity: " + entity.toString());
		}
	}

	/**
	 * Find an OSM entity.
	 * <p>
	 * <b>Warning:</b> please note that finding {@link Relation} and
	 * {@link Bound} is not yet supported. Trying to find such {@link Entity}
	 * causes this method to throw an {@link UnsupportedOperationException}.
	 * 
	 * @param osmId
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
	public <T extends Entity> T find(long osmId, Class<T> entityClass) {
		if (entityClass == null) throw new IllegalArgumentException("You must provide an Entity class");
		else if (entityClass.equals(Node.class)) return (T) findNode(osmId);
		else if (entityClass.equals(Way.class)) return (T) findWay(osmId);
		else if (entityClass.equals(Relation.class)) return (T) findRelation(osmId);
		else if (entityClass.equals(Bound.class)) return (T) findBound(osmId);
		else throw new IllegalArgumentException(entityClass.getSimpleName() + " is not valid");
	}

	protected Node findNode(long osmId) {
		try {
			SearchResponse searchResponse = client.prepareSearch(indexName)
					.setQuery(QueryBuilders.idsQuery(EntityDao.NODE).ids(Long.toString(osmId)))
					.addFields("location", "tags")
					.execute().actionGet();
			return searchResponse.getHits().getTotalHits() != 1 ? null :
					entityMapper.unmarshallNode(searchResponse.getHits().getAt(0));
		} catch (Exception e) {
			throw new DaoException("Unable to find Node [" + osmId + "]", e);
		}
	}

	protected Way findWay(long osmId) {
		try {
			SearchResponse searchResponse = client.prepareSearch(indexName)
					.setQuery(QueryBuilders.idsQuery(EntityDao.WAY).ids(Long.toString(osmId)))
					.addFields("tags", "nodes")
					.execute().actionGet();
			return searchResponse.getHits().getTotalHits() != 1 ? null :
					entityMapper.unmarshallWay(searchResponse.getHits().getAt(0));
		} catch (Exception e) {
			throw new DaoException("Unable to find Way [" + osmId + "]", e);
		}
	}

	protected Relation findRelation(long osmId) {
		throw new UnsupportedOperationException("Find Relation is not yet supported");
	}

	protected Bound findBound(long osmId) {
		throw new UnsupportedOperationException("Find Bound is not yet supported");
	}

	/**
	 * Find all OSM entities.
	 * <p>
	 * <b>Warning:</b> all objects are retrieved from elasticsearch and mounted
	 * in memory. In case of large OSM data sets, ensure you have allocated
	 * enough heap. If you already know what ids to retrieve, please consider
	 * the {@link #findAll(Class, long...)} method instead.
	 * <p>
	 * <b>Warning:</b> please note that finding all {@link Relation} and
	 * {@link Bound} is not yet supported. Trying to find such {@link Entity}
	 * causes this method to throw an {@link UnsupportedOperationException}.
	 * 
	 * @param entityClass
	 *            the class (among {@link Node}, {@link Way}, {@link Relation}
	 *            and {@link Bound}) of the Entities
	 * @param osmIds
	 *            an array of OSM id that identifies the Entities. if null, all
	 *            Entities will be retrieved (be aware of your heap size)
	 * @return The Entity objects as list or empty list if no was found
	 * @throws IllegalArgumentException
	 *             if the provided entityClass is null or invalid
	 * @throws DaoException
	 *             if something was wrong during the elasticsearch request
	 */
	@SuppressWarnings("unchecked")
	public <T extends Entity> List<T> findAll(Class<T> entityClass, long... osmIds) {
		if (entityClass == null) throw new IllegalArgumentException("You must provide an Entity class");
		else if (osmIds == null || osmIds.length == 0) throw new IllegalArgumentException("You must provide at lease 1 id");
		else if (entityClass.equals(Node.class)) return (List<T>) findAllNodes(osmIds);
		else if (entityClass.equals(Way.class)) return (List<T>) findAllWays(osmIds);
		else if (entityClass.equals(Relation.class)) return (List<T>) findAllRelations(osmIds);
		else if (entityClass.equals(Bound.class)) return (List<T>) findAllBounds(osmIds);
		else throw new IllegalArgumentException(entityClass.getSimpleName() + " is not valid");
	}

	protected List<Node> findAllNodes(long... osmIds) {
		try {
			// Build query
			MultiGetRequestBuilder requestBuilder = client.prepareMultiGet();
			for (long osmId : osmIds) {
				Item item = new Item(indexName, EntityDao.NODE, String.valueOf(osmId)).fields("location", "tags");
				requestBuilder.add(item);
			}
			// Execute query
			MultiGetResponse response = requestBuilder.execute().actionGet();
			// Process result
			List<Node> nodes = new ArrayList<Node>();
			for (MultiGetItemResponse item : response.responses()) {
				if (item.failed()) throw new DaoException("Unable to get Node [" + item.id() + "]: " + item.failure().message());
				Node node = entityMapper.unmarshallNode(item.response());
				nodes.add(node);
			}
			return nodes;
		} catch (Exception e) {
			throw new DaoException("Unable to find Nodes " + Arrays.toString(osmIds), e);
		}
	}

	protected List<Way> findAllWays(long... osmIds) {
		try {
			// Build query
			MultiGetRequestBuilder requestBuilder = client.prepareMultiGet();
			for (long osmId : osmIds) {
				Item item = new Item(indexName, EntityDao.WAY, String.valueOf(osmId)).fields("tags", "nodes");
				requestBuilder.add(item);
			}
			// Execute query
			MultiGetResponse response = requestBuilder.execute().actionGet();
			// Process result
			List<Way> ways = new ArrayList<Way>();
			for (MultiGetItemResponse item : response.responses()) {
				if (item.failed()) throw new DaoException("Unable to get Way [" + item.id() + "]: " + item.failure().message());
				Way way = entityMapper.unmarshallWay(item.response());
				ways.add(way);
			}
			return ways;
		} catch (Exception e) {
			throw new DaoException("Unable to find Ways " + Arrays.toString(osmIds), e);
		}
	}

	protected List<Relation> findAllRelations(long... osmIds) {
		throw new UnsupportedOperationException("Find all Relations is not yet supported");
	}

	protected List<Bound> findAllBounds(long... osmIds) {
		throw new UnsupportedOperationException("Find all Bounds is not yet supported");
	}

	/**
	 * Delete an OSM entity.
	 * <p>
	 * <b>Warning:</b> please note that deleting {@link Relation} and
	 * {@link Bound} is not yet supported. Trying to delete such {@link Entity}
	 * causes this method to throw an {@link UnsupportedOperationException}.
	 * 
	 * @param osmId
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
	public <T extends Entity> boolean delete(long osmId, Class<T> entityClass) {
		if (entityClass == null) throw new IllegalArgumentException("You must provide an Entity class");
		else if (entityClass.equals(Node.class)) return deleteEntity(osmId, EntityDao.NODE);
		else if (entityClass.equals(Way.class)) return deleteEntity(osmId, EntityDao.WAY);
		else if (entityClass.equals(Relation.class)) throw new UnsupportedOperationException("Delete Relation is not yet supported");
		else if (entityClass.equals(Bound.class)) throw new UnsupportedOperationException("Delete Bound is not yet supported");
		else throw new IllegalArgumentException(entityClass.getSimpleName() + " is not valid");
	}

	protected boolean deleteEntity(long osmId, String type) {
		try {
			DeleteResponse response = client.prepareDelete(indexName, type, Long.toString(osmId))
					.execute().actionGet();
			return !response.notFound();
		} catch (Exception e) {
			throw new DaoException("Unable to delete " + type + " [" + osmId + "]", e);
		}
	}

}
