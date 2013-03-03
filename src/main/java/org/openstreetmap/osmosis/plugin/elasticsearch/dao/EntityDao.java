package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.openstreetmap.osmosis.core.domain.v0_6.Bound;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
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
		if (entity == null) throw new IllegalArgumentException("You must provide a non-null Entity");
		try {
			buildIndexRequest(entity).execute().actionGet();
		} catch (Exception e) {
			throw new DaoException("Unable to save Entity [" + entity + "]", e);
		}
	}

	/**
	 * Save (index) asynchronously all OSM Entities using a bulk request.
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
				LOG.warning(String.format("Unable to add Entity [%s] to bulk request, cause: %s",
						entity, exception.getMessage()));
			}
		}
		bulkRequest.execute(new ActionListener<BulkResponse>() {
			@Override
			public void onResponse(BulkResponse bulkResponse) {
				if (!bulkResponse.hasFailures()) return;
				for (BulkItemResponse response : bulkResponse.items()) {
					if (!response.failed()) continue;
					LOG.warning(String.format("Unable to index Entity [index=%s, type=%s, id=%s], cause: %s",
							response.index(), response.type(), response.id(), response.failureMessage()));
				}
			}

			@Override
			public void onFailure(Throwable throwable) {
				LOG.severe("Unable to save all entities, cause: " + throwable.getMessage());
			}
		});
	}

	protected IndexRequestBuilder buildIndexRequest(Entity entity) throws IOException {
		switch (entity.getType()) {
		case Node:
			return client.prepareIndex(indexName, EntityDao.NODE, Long.toString(entity.getId()))
					.setSource(entityMapper.marshall(entity));
		case Way:
			return client.prepareIndex(indexName, EntityDao.WAY, Long.toString(entity.getId()))
					.setSource(entityMapper.marshall(entity));
		case Relation:
			throw new UnsupportedOperationException("Save Relation is not yet supported");
		case Bound:
			throw new UnsupportedOperationException("Save Bound is not yet supported");
		default:
			throw new IllegalArgumentException("Unknown EntityType for entity: " + entity);
		}
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
	@SuppressWarnings("unchecked")
	public <T extends Entity> T find(Class<T> entityClass, long osmId) {
		EntityType type = entityClassToType(entityClass);
		try {
			Item item = buildGetItemRequest(type, osmId);
			GetResponse response = client.prepareGet(item.index(), item.type(), item.id())
					.setFields(item.fields())
					.execute().actionGet();
			return response.exists() ? (T) entityMapper.unmarshall(type, response) : null;
		} catch (Exception e) {
			throw new DaoException("Unable to find " + type + " with id " + osmId, e);
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
	@SuppressWarnings("unchecked")
	public <T extends Entity> List<T> findAll(Class<T> entityClass, long... osmIds) {
		EntityType type = entityClassToType(entityClass);
		try {
			// Build request
			MultiGetRequestBuilder request = client.prepareMultiGet();
			for (long osmId : osmIds) {
				request.add(buildGetItemRequest(type, osmId));
			}
			// Compute response
			MultiGetResponse responses = request.execute().actionGet();
			List<T> entities = new ArrayList<T>();
			for (MultiGetItemResponse item : responses) {
				if (item.failed()) throw new ElasticSearchException(item.failure().message());
				entities.add((T) entityMapper.unmarshall(type, item.response()));
			}
			return entities;
		} catch (Exception e) {
			throw new DaoException("Unable to findAll " + type + " with ids " + Arrays.toString(osmIds), e);
		}
	}

	protected Item buildGetItemRequest(EntityType type, long osmId) {
		switch (type) {
		case Node:
			return new Item(indexName, EntityDao.NODE, String.valueOf(osmId)).fields("location", "tags");
		case Way:
			return new Item(indexName, EntityDao.WAY, String.valueOf(osmId)).fields("tags", "nodes");
		case Relation:
			throw new UnsupportedOperationException("Get Relation is not yet supported");
		case Bound:
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
	public <T extends Entity> boolean delete(Class<T> entityClass, long osmId) {
		EntityType type = entityClassToType(entityClass);
		try {
			return !buildDeleteRequest(type, osmId).execute().actionGet().notFound();
		} catch (Exception e) {
			throw new DaoException("Unable to delete " + type + " with id " + osmId, e);
		}
	}

	protected DeleteRequestBuilder buildDeleteRequest(EntityType type, long osmId) {
		switch (type) {
		case Node:
			return client.prepareDelete(indexName, EntityDao.NODE, Long.toString(osmId));
		case Way:
			return client.prepareDelete(indexName, EntityDao.WAY, Long.toString(osmId));
		case Relation:
			throw new UnsupportedOperationException("Delete Relation is not yet supported");
		case Bound:
			throw new UnsupportedOperationException("Delete Bound is not yet supported");
		default:
			throw new IllegalArgumentException("Unknown EntityType " + type);
		}
	}

	protected EntityType entityClassToType(Class<? extends Entity> entityClass) {
		if (entityClass == null) throw new IllegalArgumentException("Provided Entity class is null");
		else if (entityClass.equals(Node.class)) return EntityType.Node;
		else if (entityClass.equals(Way.class)) return EntityType.Way;
		else if (entityClass.equals(Relation.class)) return EntityType.Relation;
		else if (entityClass.equals(Bound.class)) return EntityType.Bound;
		else throw new IllegalArgumentException(entityClass.getSimpleName() + " is not a known Entity");
	}

}
