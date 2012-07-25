package org.openstreetmap.osmosis.plugin.elasticsearch;

import org.elasticsearch.client.Client;
import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.SinkManager;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.plugin.elasticsearch.client.ElasticsearchClientBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexService;

public class ElasticSearchWriterFactory extends TaskManagerFactory {

	public static final String PARAM_CLUSTER_NAME = "clusterName";
	public static final String PARAM_IS_NODE_CLIENT = "isNodeClient";
	public static final String PARAM_HOST = "host";
	public static final String PARAM_PORT = "port";
	public static final String PARAM_INDEX_NAME = "indexName";
	public static final String PARAM_CREATE_INDEX = "createIndex";

	@Override
	protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {
		// Build ElasticSearch client
		ElasticsearchClientBuilder clientBuilder = new ElasticsearchClientBuilder();

		clientBuilder.clusterName = getStringArgument(taskConfig, PARAM_CLUSTER_NAME,
				getDefaultStringArgument(taskConfig, "elasticsearch"));

		clientBuilder.isNodeClient = getBooleanArgument(taskConfig, PARAM_IS_NODE_CLIENT, false);

		clientBuilder.host = getStringArgument(taskConfig, PARAM_HOST,
				getDefaultStringArgument(taskConfig, "localhost"));

		clientBuilder.port = getIntegerArgument(taskConfig, PARAM_PORT,
				getDefaultIntegerArgument(taskConfig, 9300));

		Client client = clientBuilder.build();

		// Build EntityDao
		String indexName = getStringArgument(taskConfig, PARAM_INDEX_NAME,
				getDefaultStringArgument(taskConfig, "osm"));

		Boolean createIndex = getBooleanArgument(taskConfig, PARAM_CREATE_INDEX, false);
		IndexService indexService = new IndexService(client);
		if (createIndex) indexService.createIndex(indexName);
		EntityDao entityDao = new EntityDao(indexService, indexName);

		// Return the SinkManager
		Sink sink = new ElasticSearchWriterTask(entityDao);
		return new SinkManager(taskConfig.getId(), sink, taskConfig.getPipeArgs());
	}

}
