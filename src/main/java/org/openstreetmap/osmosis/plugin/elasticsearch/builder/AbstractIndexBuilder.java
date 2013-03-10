package org.openstreetmap.osmosis.plugin.elasticsearch.builder;

import org.elasticsearch.client.Client;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Endpoint;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Parameters;

public abstract class AbstractIndexBuilder {

	private final Endpoint endpoint;
	private final Parameters params;

	public AbstractIndexBuilder(Endpoint endpoint, Parameters params) {
		this.endpoint = endpoint;
		this.params = params;
	}

	public void createIndex() {
		int shards = Integer.valueOf(params.getProperty(getSpecializedIndexSuffix() + ".settings.shards"));
		int replicas = Integer.valueOf(params.getProperty(getSpecializedIndexSuffix() + ".settings.replicas"));
		String mappings = params.getProperty(getSpecializedIndexSuffix() + ".mappings");
		endpoint.getIndexAdminService().createIndex(getSpecializedIndexName(), shards, replicas, mappings);
	}

	/**
	 * @return A {@link Client} connected to elasticsearch to execute requests.
	 */
	protected Client getClient() {
		return endpoint.getClient();
	}

	/**
	 * @return An {@link EntityDao} instance to ease the access to the main
	 *         entity index.
	 */
	protected EntityDao getEntityDao() {
		return endpoint.getEntityDao();
	}

	/**
	 * @return A {@link Parameters} object to access plugin's parameters.
	 */
	protected Parameters getParameters() {
		return params;
	}

	/**
	 * @return The specialized index name to use
	 */
	public String getSpecializedIndexName() {
		return getEntityIndexName() + "-" + getSpecializedIndexSuffix();
	}

	/**
	 * @return The Entity index name to use
	 */
	protected String getEntityIndexName() {
		return params.getProperty(Parameters.INDEX_NAME);
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
