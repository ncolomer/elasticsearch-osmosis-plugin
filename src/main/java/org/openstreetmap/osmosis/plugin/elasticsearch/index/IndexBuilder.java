package org.openstreetmap.osmosis.plugin.elasticsearch.index;

import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexService;

public interface IndexBuilder {

	/**
	 * This method should return a short name describing the index to create.
	 * <p>
	 * It will be appended to the indexName provided by the user.
	 * 
	 * @return A {@link String}
	 */
	public String getIndexName();

	/**
	 * This method should return a A map of elasticsearch Type / Mapping pair
	 * representing the desirated index mapping.
	 * <p>
	 * The key represents the target type and the {@link XContentBuilder} value
	 * contains the mapping as JSON.
	 * <p>
	 * Note that the returned mapping will be created using the
	 * {@link IndexService#createIndex(String, Map)} method which itself uses
	 * the {@link CreateIndexRequestBuilder#addMapping(String, Map)} builder
	 * internally. See the elasticsearch <a
	 * href="http://www.elasticsearch.org/guide/reference/mapping/">mapping
	 * reference</a> for more information.
	 * 
	 * @return A {@link Map}
	 */
	public Map<String, XContentBuilder> getIndexMapping();

	/**
	 * This method should construct the specialized index.
	 * <p>
	 * It is called after the OSM index was built and the desirated index is
	 * created (using {@link #getIndexName()} and {@link #getIndexMapping()}
	 * methods).
	 * 
	 * @param indexService
	 *            An {@link IndexService} instance to use when building the
	 *            specialized index.
	 */
	public void buildIndex(IndexService indexService);

}
