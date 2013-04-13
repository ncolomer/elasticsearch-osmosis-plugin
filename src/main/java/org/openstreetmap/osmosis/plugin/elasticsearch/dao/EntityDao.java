package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;
import org.openstreetmap.osmosis.core.domain.v0_6.Bound;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESEntity;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESEntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESWay;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESShape;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESShape.ESShapeBuilder;

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
		saveAll(Arrays.asList(entity));
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
	public <T extends Entity> void saveAll(List<T> entities) {
		if (entities == null || entities.isEmpty()) return;
		List<Node> nodes = new ArrayList<Node>();
		List<Way> ways = new ArrayList<Way>();
		for (T entity : entities) {
			if (entity == null) continue;
			switch (entity.getType()) {
			case Node:
				nodes.add((Node) entity);
				break;
			case Way:
				ways.add((Way) entity);
				break;
			case Relation:
			case Bound:
			default:
				LOG.warning(String.format("Unable to add Entity %s to bulk request, " +
						"cause: save %s is not yet supported", entity, entity.getType()));
			}
		}
		if (!nodes.isEmpty()) saveAllNodes(nodes);
		if (!ways.isEmpty()) saveAllWays(ways);
	}

	protected void saveAllNodes(List<Node> nodes) {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (Node node : nodes) {
			try {
				ESNode esNode = ESNode.Builder.buildFromEntity(node);
				bulkRequest.add(client.prepareIndex(indexName, esNode.getType().getIndiceName(), esNode.getIdString())
						.setSource(esNode.toJson()));
			} catch (Exception exception) {
				LOG.warning(String.format("Unable to add Entity %s to bulk request, cause: %s",
						node.getId(), exception.getMessage()));
			}
		}
		executeBulkRequest(bulkRequest);
	}

	protected void saveAllWays(List<Way> ways) {
		Iterator<MultiGetItemResponse> iterator = getNodeItems(ways);
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (Way way : ways) {
			try {
				int size = way.getWayNodes().size();
				ESShape shape = getShape(iterator, size);
				ESWay esWay = ESWay.Builder.buildFromEntity(way, shape);
				bulkRequest.add(client.prepareIndex(indexName, esWay.getType().getIndiceName(), esWay.getIdString())
						.setSource(esWay.toJson()));
			} catch (Exception e) {
				LOG.warning(String.format("Unable to add Entity %s to bulk request, cause: %s",
						way.getId(), e.getMessage()));
			}
		}
		executeBulkRequest(bulkRequest);
	}

	protected Iterator<MultiGetItemResponse> getNodeItems(List<Way> ways) {
		MultiGetRequestBuilder request = client.prepareMultiGet();
		for (Way way : ways) {
			for (WayNode wayNode : way.getWayNodes()) {
				request.add(new Item(indexName, ESEntityType.NODE.getIndiceName(),
						String.valueOf(wayNode.getNodeId())).fields("shape"));
			}
		}
		MultiGetResponse responses = request.execute().actionGet();
		Iterator<MultiGetItemResponse> iterator = responses.iterator();
		return iterator;
	}

	protected ESShape getShape(Iterator<MultiGetItemResponse> iterator, int size) {
		ESShapeBuilder shapeBuilder = new ESShapeBuilder(size);
		for (int i = 0; i < size; i++) {
			GetResponse response = iterator.next().getResponse();
			if (!response.isExists()) continue;
			@SuppressWarnings("unchecked")
			Map<String, Object> shape = (Map<String, Object>) response.getField("shape").getValue();
			@SuppressWarnings("unchecked")
			List<Double> coordinates = (List<Double>) shape.get("coordinates");
			shapeBuilder.addLocation(coordinates.get(1), coordinates.get(0));
		}
		return shapeBuilder.build();
	}

	protected void executeBulkRequest(BulkRequestBuilder bulkRequest) {
		if (bulkRequest.numberOfActions() == 0) return;
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (!bulkResponse.hasFailures()) return;
		for (BulkItemResponse response : bulkResponse) {
			if (!response.isFailed()) continue;
			LOG.warning(String.format("Unable to save Entity %s in %s/%s, cause: %s",
					response.getId(), response.getIndex(), response.getType(), response.getFailureMessage()));
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
	public <T extends ESEntity> T find(Class<T> entityClass, long osmId) {
		return findAll(entityClass, osmId).get(0);
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
		if (osmIds == null || osmIds.length == 0) return Collections.unmodifiableList(new ArrayList<T>(0));
		try {
			MultiGetRequestBuilder request = buildMultiGetRequest(entityClass, osmIds);
			return executeMultiGetRequest(entityClass, request);
		} catch (Exception e) {
			if (e instanceof DaoException) throw (DaoException) e;
			String indiceName = ESEntityType.valueOf(entityClass).getIndiceName();
			throw new DaoException("Unable to find all " + indiceName + " entities", e);
		}
	}

	protected <T extends ESEntity> MultiGetRequestBuilder buildMultiGetRequest(Class<T> entityClass, long... osmIds) {
		ESEntityType type = ESEntityType.valueOf(entityClass);
		MultiGetRequestBuilder request = client.prepareMultiGet();
		for (long osmId : osmIds) {
			request.add(new Item(indexName, type.getIndiceName(), String.valueOf(osmId)).fields("shape", "tags"));
		}
		return request;
	}

	protected <T extends ESEntity> List<T> executeMultiGetRequest(Class<T> entityClass, MultiGetRequestBuilder request) {
		MultiGetResponse responses = request.execute().actionGet();
		List<T> entities = new ArrayList<T>();
		for (MultiGetItemResponse item : responses) {
			entities.add(buildEntityFromGetResponse(entityClass, item));
		}
		return Collections.unmodifiableList(entities);
	}

	@SuppressWarnings("unchecked")
	protected <T extends ESEntity> T buildEntityFromGetResponse(Class<T> entityClass, MultiGetItemResponse item) {
		GetResponse response = item.getResponse();
		if (!response.isExists()) throw new DaoException(String.format(
				"Entity %s does not exist in %s/%s", response.getId(),
				response.getIndex(), response.getType()));
		if (entityClass == null) throw new IllegalArgumentException("Provided Entity class is null");
		else if (entityClass.equals(ESNode.class)) return (T) ESNode.Builder.buildFromGetReponse(response);
		else if (entityClass.equals(ESWay.class)) return (T) ESWay.Builder.buildFromGetReponse(response);
		else throw new IllegalArgumentException(entityClass.getSimpleName() + " is not a known Entity");
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
					.execute().actionGet().isNotFound();
		} catch (Exception e) {
			String indiceName = ESEntityType.valueOf(entityClass).getIndiceName();
			String message = String.format("Unable to delete entity %s in %s/%s",
					osmId, indexName, indiceName);
			throw new DaoException(message, e);
		}
	}

}
