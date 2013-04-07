package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.openstreetmap.osmosis.core.domain.v0_6.Bound;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.ESEntity;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.ESEntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.ESNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.ESWay;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.LocationArrayBuilder;

public class EntityDao {

	private static final Logger LOG = Logger.getLogger(EntityDao.class.getName());

	private final String indexName;
	private final Client client;

	public EntityDao(String indexName, Client client) {
		this.indexName = indexName;
		this.client = client;
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
		if (entity == null) throw new IllegalArgumentException("You must provide a non-null Entity");
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
	 * @throws DaoException
	 *             if something was wrong during the save process
	 */
	public void saveAll(List<Entity> entities) {
		if (entities == null || entities.isEmpty()) return;
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (Entity entity : entities) {
			try {
				bulkRequest.add(buildIndexRequest(entity));
			} catch (Exception exception) {
				LOG.warning(String.format("Unable to add Entity %s to bulk request, cause: %s",
						entity.getId(), exception.getMessage()));
			}
		}
		if (bulkRequest.numberOfActions() == 0) return;
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (!bulkResponse.hasFailures()) return;
		for (BulkItemResponse response : bulkResponse.items()) {
			if (!response.failed()) continue;
			LOG.warning(String.format("Unable to save Entity %s in %s/%s, cause: %s",
					response.id(), response.index(), response.type(), response.failureMessage()));
		}
	}

	protected IndexRequestBuilder buildIndexRequest(Entity entity) {
		switch (entity.getType()) {
		case Node:
			ESNode esNode = ESNode.Builder.buildFromEntity((Node) entity);
			return client.prepareIndex(indexName, esNode.getType().getIndiceName(), esNode.getIdString())
					.setSource(esNode.toJson());
		case Way:
			Way way = (Way) entity;
			List<ESNode> nodes = findAll(ESNode.class, getNodeIds(way.getWayNodes()));
			ESWay esWay = ESWay.Builder.buildFromEntity(way, getLocationArrayBuilder(nodes));
			return client.prepareIndex(indexName, esWay.getType().getIndiceName(), esWay.getIdString())
					.setSource(esWay.toJson());
		case Relation:
			throw new UnsupportedOperationException("Save Relation is not yet supported");
		case Bound:
			throw new UnsupportedOperationException("Save Bound is not yet supported");
		default:
			throw new IllegalArgumentException("Unknown EntityType for entity: " + entity);
		}
	}

	protected LocationArrayBuilder getLocationArrayBuilder(List<ESNode> nodes) {
		LocationArrayBuilder builder = new LocationArrayBuilder(nodes.size());
		for (ESNode esNode : nodes) {
			builder.addLocation(esNode.getLatitude(), esNode.getLongitude());
		}
		return builder;
	}

	protected long[] getNodeIds(List<WayNode> list) {
		long[] nodeIds = new long[list.size()];
		for (int i = 0; i < list.size(); i++) {
			nodeIds[i] = list.get(i).getNodeId();
		}
		return nodeIds;
	}

	/**
	 * Find an OSM entity.
	 * <p>
	 * <b>Warning:</b> please note that finding {@link Relation} and
	 * {@link Bound} is not yet supported. Trying to find such {@link Entity}
	 * causes this method to throw an {@link UnsupportedOperationException}.
	 * 
	 * @param entityClass
	 *            the class (among {@link Node}, {@link Way}, {@link Relation}
	 *            and {@link Bound}) of the Entity
	 * @param osmId
	 *            the OSM id that identifies the Entity
	 * @return The Entity object, null if not found
	 * @throws IllegalArgumentException
	 *             if the provided entityClass is null or invalid
	 * @throws DaoException
	 *             if something was wrong during the elasticsearch request
	 */
	public <T extends ESEntity> T find(Class<T> entityClass, long osmId) {
		try {
			ESEntityType type = ESEntityType.valueOf(entityClass);
			Item item = buildGetItemRequest(type, osmId);
			GetResponse response = client.prepareGet(item.index(), item.type(), item.id())
					.setFields(item.fields())
					.execute().actionGet();
			return response.exists() ? (T) buildFromGetReponse(entityClass, response) : null;
		} catch (Exception e) {
			String indiceName = ESEntityType.valueOf(entityClass).getIndiceName();
			String message = String.format("Unable to find Entity %s in %s/%s",
					osmId, indexName, indiceName);
			throw new DaoException(message, e);
		}
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
	 * @return The Entity objects as list if all ids was found
	 * @throws IllegalArgumentException
	 *             if the provided entityClass is null or invalid
	 * @throws DaoException
	 *             if something was wrong during the elasticsearch request
	 */
	public <T extends ESEntity> List<T> findAll(Class<T> entityClass, long... osmIds) {
		try {
			// Build request
			ESEntityType type = ESEntityType.valueOf(entityClass);
			MultiGetRequestBuilder request = client.prepareMultiGet();
			for (long osmId : osmIds) {
				request.add(buildGetItemRequest(type, osmId));
			}
			// Compute response
			MultiGetResponse responses = request.execute().actionGet();
			List<T> entities = new ArrayList<T>();
			for (MultiGetItemResponse item : responses) {
				GetResponse response = item.getResponse();
				if (!response.exists()) throw new DaoException(String.format(
						"Entity %s does not exist in %s/%s", response.getId(),
						response.getIndex(), response.getType()));
				entities.add((T) buildFromGetReponse(entityClass, response));
			}
			return entities;
		} catch (Exception e) {
			String indiceName = ESEntityType.valueOf(entityClass).getIndiceName();
			throw new DaoException("Unable to find all " + indiceName + " entities", e);
		}
	}

	protected Item buildGetItemRequest(ESEntityType type, long osmId) {
		switch (type) {
		case NODE:
			return new Item(indexName, ESEntityType.NODE.getIndiceName(), String.valueOf(osmId)).fields("shape", "tags");
		case WAY:
			return new Item(indexName, ESEntityType.WAY.getIndiceName(), String.valueOf(osmId)).fields("shape", "tags");
		case RELATION:
			throw new UnsupportedOperationException("Get Relation is not yet supported");
		case BOUND:
			throw new UnsupportedOperationException("Get Bound is not yet supported");
		default:
			throw new IllegalArgumentException("Unknown EntityType " + type);
		}
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
	public <T extends ESEntity> boolean delete(Class<T> entityClass, long osmId) {
		try {
			String indiceName = ESEntityType.valueOf(entityClass).getIndiceName();
			return !client.prepareDelete(indexName, indiceName, Long.toString(osmId))
					.execute().actionGet().notFound();
		} catch (Exception e) {
			String indiceName = ESEntityType.valueOf(entityClass).getIndiceName();
			String message = String.format("Unable to delete entity %s in %s/%s",
					osmId, indexName, indiceName);
			throw new DaoException(message, e);
		}
	}

	@SuppressWarnings("unchecked")
	protected <T extends ESEntity> T buildFromGetReponse(Class<T> entityClass, GetResponse response) {
		if (entityClass == null) throw new IllegalArgumentException("Provided Entity class is null");
		else if (entityClass.equals(ESNode.class)) return (T) ESNode.Builder.buildFromGetReponse(response);
		else if (entityClass.equals(ESWay.class)) return (T) ESWay.Builder.buildFromGetReponse(response);
		else throw new IllegalArgumentException(entityClass.getSimpleName() + " is not a known Entity");
	}

}
