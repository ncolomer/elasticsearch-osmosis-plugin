package org.openstreetmap.osmosis.plugin.elasticsearch;

import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.client.Client;
import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.SinkManager;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.plugin.elasticsearch.client.ElasticsearchClientBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.index.SpecialiazedIndex;
import org.openstreetmap.osmosis.plugin.elasticsearch.index.osm.OsmIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexService;

public class ElasticSearchWriterFactory extends TaskManagerFactory {

	public static final String PARAM_CLUSTER_NAME = "clusterName";
	public static final String PARAM_IS_NODE_CLIENT = "isNodeClient";
	public static final String PARAM_HOST = "host";
	public static final String PARAM_PORT = "port";
	public static final String PARAM_INDEX_NAME = "indexName";
	public static final String PARAM_CREATE_INDEX = "createIndex";
	public static final String PARAM_SPECIALIZED_INDEX = "specializedIndexex";

	@Override
	protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {
		// Build ElasticSearch client
		Client client = buildElasticsearchClient(taskConfig);
		// Build IndexService
		IndexService indexService = new IndexService(client);
		// Build EntityDao
		EntityDao entityDao = buildEntityDao(taskConfig, indexService);
		// Get specialized index to build
		Set<SpecialiazedIndex> specIndexes = getWantedSpecializedIndexes(taskConfig);
		// Return the SinkManager
		Sink sink = new ElasticSearchWriterTask(indexService, entityDao, specIndexes);
		return new SinkManager(taskConfig.getId(), sink, taskConfig.getPipeArgs());
	}

	protected Client buildElasticsearchClient(TaskConfiguration taskConfig) {
		ElasticsearchClientBuilder clientBuilder = new ElasticsearchClientBuilder();
		clientBuilder.clusterName = getStringArgument(taskConfig, PARAM_CLUSTER_NAME,
				getDefaultStringArgument(taskConfig, "elasticsearch"));
		clientBuilder.isNodeClient = getBooleanArgument(taskConfig, PARAM_IS_NODE_CLIENT, false);
		clientBuilder.host = getStringArgument(taskConfig, PARAM_HOST,
				getDefaultStringArgument(taskConfig, "localhost"));
		clientBuilder.port = getIntegerArgument(taskConfig, PARAM_PORT,
				getDefaultIntegerArgument(taskConfig, 9300));
		return clientBuilder.build();
	}

	protected EntityDao buildEntityDao(TaskConfiguration taskConfig, IndexService indexService) {
		String indexName = getStringArgument(taskConfig, PARAM_INDEX_NAME,
				getDefaultStringArgument(taskConfig, "osm"));
		Boolean createIndex = getBooleanArgument(taskConfig, PARAM_CREATE_INDEX, false);
		if (createIndex) indexService.createIndex(indexName, new OsmIndexBuilder().getIndexMapping());
		EntityDao entityDao = new EntityDao(indexName, indexService);
		return entityDao;
	}

	protected Set<SpecialiazedIndex> getWantedSpecializedIndexes(TaskConfiguration taskConfig) {
		Set<SpecialiazedIndex> values = new HashSet<SpecialiazedIndex>();
		String array = getStringArgument(taskConfig, PARAM_SPECIALIZED_INDEX,
				getDefaultStringArgument(taskConfig, ""));
		for (String item : array.split(",")) {
			try {
				values.add(SpecialiazedIndex.valueOf(item.toUpperCase()));
			} catch (Exception e) {}
		}
		return values;
	}

}
