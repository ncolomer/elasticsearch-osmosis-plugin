package org.openstreetmap.osmosis.plugin.elasticsearch.index;

import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;

public abstract class AbstractIndexBuilder {

	private final Client client;
	private final EntityDao entityDao;
	private final String indexName;

	public AbstractIndexBuilder(Client client, EntityDao entityDao, String indexName) {
		this.client = client;
		this.entityDao = entityDao;
		this.indexName = indexName;
	}

	/**
	 * @return A {@link Client} connected to elasticsearch to execute requests
	 */
	protected Client getClient() {
		return client;
	}

	/**
	 * @return An {@link EntityDao} instance to ease access to already indexed
	 *         OSM data
	 */
	protected EntityDao getEntityDao() {
		return entityDao;
	}

	/**
	 * @return The specialized index name to use
	 */
	public String getSpecializedIndexName() {
		return indexName + "-" + getSpecializedIndexSuffix();
	}

	/**
	 * @return The Entity index name to use
	 */
	protected String getEntityIndexName() {
		return indexName;
	}

	/**
	 * This method should return a short name describing the index to create.
	 * <p>
	 * It will be appended to the indexName provided by the user.
	 * 
	 * @return A {@link String}
	 */
	public abstract String getSpecializedIndexSuffix();

	/**
	 * This method should return a A map of elasticsearch Type / Mapping pair
	 * representing the desirated index mapping.
	 * <p>
	 * The key represents the target type and the {@link XContentBuilder} value
	 * contains the mapping as JSON.
	 * <p>
	 * Note that the returned mapping will be created using the
	 * {@link IndexAdminService#createIndex(String, Map)} method which itself
	 * uses the {@link CreateIndexRequestBuilder#addMapping(String, Map)}
	 * builder internally. See the elasticsearch <a
	 * href="http://www.elasticsearch.org/guide/reference/mapping/">mapping
	 * reference</a> for more information.
	 * 
	 * @return A {@link Map}
	 */
	public abstract Map<String, XContentBuilder> getIndexMapping();

	/**
	 * This method should construct the specialized index.
	 * <p>
	 * It is called after the OSM index was built and the specialized index was
	 * created (using {@link #getSpecializedIndexSuffix()} and
	 * {@link #getIndexMapping()} methods).
	 */
	public abstract void buildIndex();

}
