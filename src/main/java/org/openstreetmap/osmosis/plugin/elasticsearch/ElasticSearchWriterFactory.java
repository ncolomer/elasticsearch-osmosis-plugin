package org.openstreetmap.osmosis.plugin.elasticsearch;

import java.io.FileReader;
import java.lang.reflect.Constructor;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.elasticsearch.client.Client;
import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.SinkManager;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.plugin.elasticsearch.builder.AbstractIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.client.ElasticSearchClientBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;

public class ElasticSearchWriterFactory extends TaskManagerFactory {

	public static final String PARAM_PROPERTIES_FILE = "properties.file";
	public static final String PARAM_CLUSTER_HOSTS = "cluster.hosts";
	public static final String PARAM_CLUSTER_NAME = "cluster.name";
	public static final String PARAM_INDEX_NAME = "index.name";
	public static final String PARAM_INDEX_CREATE = "index.create";
	public static final String PARAM_INDEX_CONFIG = "index.config";
	public static final String PARAM_INDEX_BUILDERS = "index.builders";

	@Override
	protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {
		// Retrieve parameters
		Properties params = buildPluginParameters(taskConfig);
		// Build ElasticSearch client
		Client client = buildElasticsearchClient(params);
		// Build indexAdminService
		IndexAdminService indexAdminService = new IndexAdminService(client);
		// Build EntityDao
		EntityDao entityDao = buildEntityDao(indexAdminService, params);
		// Get specialized index to build
		Set<AbstractIndexBuilder> indexBuilders = getSelectedIndexBuilders(client, entityDao, params);
		// Return the SinkManager
		Sink sink = new ElasticSearchWriterTask(indexAdminService, entityDao, indexBuilders, params);
		return new SinkManager(taskConfig.getId(), sink, taskConfig.getPipeArgs());
	}

	protected Properties buildPluginParameters(TaskConfiguration taskConfig) {
		Properties params = new Properties();
		try {
			params.load(getClass().getClassLoader().getResourceAsStream("plugin.properties"));
		} catch (Exception e) {
			throw new RuntimeException("Unable to load internal plugin.properties");
		}
		if (doesArgumentExist(taskConfig, PARAM_PROPERTIES_FILE)) {
			String fileName = getStringArgument(taskConfig, PARAM_PROPERTIES_FILE);
			try {
				params.load(new FileReader(fileName));
			} catch (Exception e) {
				throw new RuntimeException("Unable to load properties file " + fileName);
			}
		}
		addArgumentIfExists(PARAM_CLUSTER_HOSTS, taskConfig, params);
		addArgumentIfExists(PARAM_CLUSTER_NAME, taskConfig, params);
		addArgumentIfExists(PARAM_INDEX_NAME, taskConfig, params);
		addArgumentIfExists(PARAM_INDEX_CREATE, taskConfig, params);
		addArgumentIfExists(PARAM_INDEX_CONFIG, taskConfig, params);
		addArgumentIfExists(PARAM_INDEX_BUILDERS, taskConfig, params);
		return params;
	}

	protected void addArgumentIfExists(String key, TaskConfiguration taskConfig, Properties properties) {
		if (doesArgumentExist(taskConfig, key)) {
			String value = getStringArgument(taskConfig, key);
			properties.put(key, value);
		}
	}

	protected Client buildElasticsearchClient(Properties params) {
		return ElasticSearchClientBuilder.newClient()
				.setClusterName(params.getProperty(PARAM_CLUSTER_NAME))
				.setHosts(params.getProperty(PARAM_CLUSTER_HOSTS))
				.build();
	}

	protected EntityDao buildEntityDao(IndexAdminService indexAdminService, Properties params) {
		String indexName = params.getProperty(PARAM_INDEX_NAME);
		Boolean createIndex = Boolean.valueOf(params.getProperty(PARAM_INDEX_CREATE));
		if (createIndex) {
			String indexConfig = params.getProperty(PARAM_INDEX_CONFIG);
			indexAdminService.createIndex(indexName, indexConfig);
		}
		EntityDao entityDao = new EntityDao(indexName, indexAdminService.getClient());
		return entityDao;
	}

	protected Set<AbstractIndexBuilder> getSelectedIndexBuilders(Client client, EntityDao entityDao, Properties params) {
		Set<AbstractIndexBuilder> set = new LinkedHashSet<AbstractIndexBuilder>();
		String entityIndexName = params.getProperty(PARAM_INDEX_NAME);
		String selectedIndexBuilders = params.getProperty(PARAM_INDEX_BUILDERS);
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
				AbstractIndexBuilder indexBuilder = _const.newInstance(client, entityDao, entityIndexName, indexConfig);
				set.add(indexBuilder);
			} catch (Exception e) {
				throw new RuntimeException("Unable to load IndexBuilder [" + indexBuilderName + "]");
			}
		}
		return set;
	}

}
