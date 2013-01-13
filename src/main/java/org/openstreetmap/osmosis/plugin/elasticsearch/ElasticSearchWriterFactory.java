package org.openstreetmap.osmosis.plugin.elasticsearch;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.elasticsearch.client.Client;
import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.SinkManager;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.plugin.elasticsearch.client.ElasticsearchClientBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.index.AbstractIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.index.osm.OsmIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;

public class ElasticSearchWriterFactory extends TaskManagerFactory {

	public static final String PARAM_CLUSTER_NAME = "clusterName";
	public static final String PARAM_IS_NODE_CLIENT = "isNodeClient";
	public static final String PARAM_HOST = "host";
	public static final String PARAM_PORT = "port";
	public static final String PARAM_INDEX_NAME = "indexName";
	public static final String PARAM_CREATE_INDEX = "createIndex";
	public static final String PARAM_INDEX_BUILDERS = "indexBuilders";

	@Override
	protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {
		// Build ElasticSearch client
		Client client = buildElasticsearchClient(taskConfig);
		// Build indexAdminService
		IndexAdminService indexAdminService = new IndexAdminService(client);
		// Build EntityDao
		EntityDao entityDao = buildEntityDao(taskConfig, indexAdminService);
		// Get specialized index to build
		Set<AbstractIndexBuilder> indexBuilders = getSelectedIndexBuilders(taskConfig);
		// Return the SinkManager
		Sink sink = new ElasticSearchWriterTask(indexAdminService, entityDao, indexBuilders);
		return new SinkManager(taskConfig.getId(), sink, taskConfig.getPipeArgs());
	}

	protected Client buildElasticsearchClient(TaskConfiguration taskConfig) {
		ElasticsearchClientBuilder clientBuilder = new ElasticsearchClientBuilder();
		clientBuilder.clusterName = getStringArgument(taskConfig, PARAM_CLUSTER_NAME,
				getDefaultStringArgument(taskConfig, "elasticsearch"));
		clientBuilder.isNodeClient = getBooleanArgument(taskConfig, PARAM_IS_NODE_CLIENT, true);
		clientBuilder.host = getStringArgument(taskConfig, PARAM_HOST,
				getDefaultStringArgument(taskConfig, "localhost"));
		clientBuilder.port = getIntegerArgument(taskConfig, PARAM_PORT,
				getDefaultIntegerArgument(taskConfig, 9300));
		return clientBuilder.build();
	}

	protected EntityDao buildEntityDao(TaskConfiguration taskConfig, IndexAdminService indexAdminService) {
		String indexName = getStringArgument(taskConfig, PARAM_INDEX_NAME,
				getDefaultStringArgument(taskConfig, "osm"));
		Boolean createIndex = getBooleanArgument(taskConfig, PARAM_CREATE_INDEX, false);
		if (createIndex) indexAdminService.createIndex(indexName, new OsmIndexBuilder().getIndexMapping());
		EntityDao entityDao = new EntityDao(indexName, indexAdminService.getClient());
		return entityDao;
	}

	protected Set<AbstractIndexBuilder> getSelectedIndexBuilders(TaskConfiguration taskConfig) {
		Properties properties = getIndexBuilderProperties();
		Set<AbstractIndexBuilder> set = new HashSet<AbstractIndexBuilder>();
		String selectedIndexBuilders = getStringArgument(taskConfig, PARAM_INDEX_BUILDERS,
				getDefaultStringArgument(taskConfig, ""));
		for (String indexBuilderName : selectedIndexBuilders.split(",")) {
			if (!properties.containsKey(indexBuilderName)) {
				throw new RuntimeException("Unable to find IndexBuilder [" + indexBuilderName + "]");
			} else try {
				String indexBuilderClass = properties.getProperty(indexBuilderName);
				AbstractIndexBuilder indexBuilder = (AbstractIndexBuilder) Class.forName(indexBuilderClass).newInstance();
				set.add(indexBuilder);
			} catch (Exception e) {
				throw new RuntimeException("Unable to load IndexBuilder [" + indexBuilderName + "]");
			}
		}
		return set;
	}

	protected Properties getIndexBuilderProperties() {
		try {
			Properties properties = new Properties();
			properties.load(this.getClass().getClassLoader().getResourceAsStream("indexBuilder.properties"));
			return properties;
		} catch (Exception e) {
			throw new RuntimeException("Unable to load IndexBuilder list");
		}
	}

}
