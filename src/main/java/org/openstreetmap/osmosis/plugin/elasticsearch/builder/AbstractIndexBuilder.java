package org.openstreetmap.osmosis.plugin.elasticsearch.builder;

import org.elasticsearch.client.Client;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;

public abstract class AbstractIndexBuilder {

	private final Client client;
	private final EntityDao entityDao;
	private final String entityIndexName;
	private final String indexConfig;

	public AbstractIndexBuilder(Client client, EntityDao entityDao, String entityIndexName, String indexConfig) {
		this.client = client;
		this.entityDao = entityDao;
		this.entityIndexName = entityIndexName;
		this.indexConfig = indexConfig;
	}

	/**
	 * @return A {@link Client} connected to elasticsearch to execute requests.
	 */
	protected Client getClient() {
		return client;
	}

	/**
	 * @return An {@link EntityDao} instance to ease the access to the main
	 *         entity index.
	 */
	protected EntityDao getEntityDao() {
		return entityDao;
	}

	/**
	 * @return The specialized index name to use
	 */
	public String getSpecializedIndexName() {
		return entityIndexName + "-" + getSpecializedIndexSuffix();
	}

	/**
	 * @return The Entity index name to use
	 */
	protected String getEntityIndexName() {
		return entityIndexName;
	}

	/**
	 * This method returns a JSON String representing the wanted index
	 * configuration.
	 * <p>
	 * Note that the returned mapping will be used to create the index using the
	 * {@link IndexAdminService#createIndex(String, String)} method.
	 * <p>
	 * See the elasticsearch <a
	 * href="http://www.elasticsearch.org/guide/reference/mapping/">mapping
	 * reference</a> and <a href=
	 * "http://www.elasticsearch.org/guide/reference/api/admin-indices-create-index.html"
	 * >index reference</a> for more information.
	 * 
	 * @return A JSON String
	 */
	public String getIndexConfig() {
		return indexConfig;
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
	 * This method should construct the specialized index.
	 * <p>
	 * It is called after the OSM index was built and the specialized index was
	 * created (using {@link #getSpecializedIndexSuffix()} and
	 * {@link #getIndexConfig()} methods).
	 */
	public abstract void buildIndex();

}
