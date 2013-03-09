package org.openstreetmap.osmosis.plugin.elasticsearch;

import java.lang.reflect.Constructor;
import java.util.LinkedHashSet;
import java.util.Set;

import org.elasticsearch.client.Client;
import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.SinkManager;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.plugin.elasticsearch.builder.AbstractIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.client.ElasticsearchClientBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Endpoint;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Parameters;

public class ElasticSearchWriterFactory extends TaskManagerFactory {

	@Override
	protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {
		// Retrieve parameters
		Parameters params = buildPluginParameters(taskConfig);
		// Build ElasticSearch client
		Client client = buildElasticsearchClient(params);
		// Build indexAdminService
		IndexAdminService indexAdminService = new IndexAdminService(client);
		// Build EntityDao
		EntityDao entityDao = buildEntityDao(client, params);
		// Create bundle
		Endpoint endpoint = new Endpoint(client, indexAdminService, entityDao);

		// Create Index
		createIndex(indexAdminService, params);
		// Get specialized index to build
		Set<AbstractIndexBuilder> indexBuilders = getSelectedIndexBuilders(endpoint, params);
		// Return the SinkManager
		Sink sink = new ElasticSearchWriterTask(endpoint, indexBuilders, params);
		return new SinkManager(taskConfig.getId(), sink, taskConfig.getPipeArgs());
	}

	protected Parameters buildPluginParameters(TaskConfiguration taskConfig) {
		Parameters.Builder builder = new Parameters.Builder();
		// Load internal plugin.properties
		builder.loadResource("plugin.properties");
		// Load custom properties file if specified
		if (doesArgumentExist(taskConfig, Parameters.PARAM_PROPERTIES_FILE)) {
			String fileName = getStringArgument(taskConfig, Parameters.PARAM_PROPERTIES_FILE);
			builder.loadFile(fileName);
		}
		// Load custom parameters
		addArgumentIfExists(Parameters.PARAM_CLUSTER_HOSTS, taskConfig, builder);
		addArgumentIfExists(Parameters.PARAM_CLUSTER_NAME, taskConfig, builder);
		addArgumentIfExists(Parameters.PARAM_INDEX_NAME, taskConfig, builder);
		addArgumentIfExists(Parameters.PARAM_INDEX_CREATE, taskConfig, builder);
		addArgumentIfExists(Parameters.PARAM_INDEX_CONFIG, taskConfig, builder);
		addArgumentIfExists(Parameters.PARAM_INDEX_BUILDERS, taskConfig, builder);
		return builder.build();
	}

	protected void addArgumentIfExists(String key, TaskConfiguration taskConfig, Parameters.Builder builder) {
		if (doesArgumentExist(taskConfig, key)) {
			String value = getStringArgument(taskConfig, key);
			builder.addParameter(key, value);
		}
	}

	protected Client buildElasticsearchClient(Parameters params) {
		return ElasticsearchClientBuilder.newClient()
				.setClusterName(params.getProperty(Parameters.PARAM_CLUSTER_NAME))
				.setHosts(params.getProperty(Parameters.PARAM_CLUSTER_HOSTS))
				.build();
	}

	protected void createIndex(IndexAdminService indexAdminService, Parameters params) {
		if (Boolean.valueOf(params.getProperty(Parameters.PARAM_INDEX_CREATE))) {
			String indexName = params.getProperty(Parameters.PARAM_INDEX_NAME);
			String indexConfig = params.getProperty(Parameters.PARAM_INDEX_CONFIG);
			indexAdminService.createIndex(indexName, indexConfig);
		}
	}

	protected EntityDao buildEntityDao(Client client, Parameters params) {
		String indexName = params.getProperty(Parameters.PARAM_INDEX_NAME);
		return new EntityDao(indexName, client);
	}

	protected Set<AbstractIndexBuilder> getSelectedIndexBuilders(Endpoint endpoint, Parameters params) {
		// Client client; EntityDao entityDao;
		Set<AbstractIndexBuilder> set = new LinkedHashSet<AbstractIndexBuilder>();
		String entityIndexName = params.getProperty(Parameters.PARAM_INDEX_NAME);
		String selectedIndexBuilders = params.getProperty(Parameters.PARAM_INDEX_BUILDERS, "");
		if (selectedIndexBuilders.isEmpty()) return set;
		for (String indexBuilderName : selectedIndexBuilders.split(",")) {
			if (!params.containsKey(indexBuilderName)) {
				throw new RuntimeException("Unable to find IndexBuilder [" + indexBuilderName + "]");
			} else try {
				String indexBuilderClass = params.getProperty(indexBuilderName);
				String indexConfig = params.getProperty(indexBuilderName + ".config");
				@SuppressWarnings("unchecked")
				Class<AbstractIndexBuilder> _class = (Class<AbstractIndexBuilder>) Class.forName(indexBuilderClass);
				Constructor<AbstractIndexBuilder> _const = _class.getDeclaredConstructor(Client.class, EntityDao.class, String.class, String.class);
				AbstractIndexBuilder indexBuilder = _const.newInstance(endpoint.getClient(), endpoint.getEntityDao(), entityIndexName, indexConfig);
				set.add(indexBuilder);
			} catch (Exception e) {
				throw new RuntimeException("Unable to load IndexBuilder [" + indexBuilderName + "]");
			}
		}
		return set;
	}

}
