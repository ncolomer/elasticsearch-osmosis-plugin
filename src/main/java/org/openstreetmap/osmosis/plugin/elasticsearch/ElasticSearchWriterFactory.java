package org.openstreetmap.osmosis.plugin.elasticsearch;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.elasticsearch.client.Client;
import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.SinkManager;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.plugin.elasticsearch.client.ElasticSearchClientBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.index.AbstractIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.index.osm.OsmIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;

public class ElasticSearchWriterFactory extends TaskManagerFactory {

	public static final String PARAM_CLUSTER_NAME = "clusterName";
	public static final String PARAM_HOSTS = "hosts";
	public static final String PARAM_INDEX_NAME = "indexName";
	public static final String PARAM_CREATE_INDEX = "createIndex";
	public static final String PARAM_INDEX_BUILDERS = "indexBuilders";

	@Override
	protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {
		// Retrieve parameters
		Properties params = getParameters(taskConfig);
		// Build ElasticSearch client
		Client client = buildElasticsearchClient(params);
		// Build indexAdminService
		IndexAdminService indexAdminService = new IndexAdminService(client);
		// Build EntityDao
		EntityDao entityDao = buildEntityDao(indexAdminService, params);
		// Get specialized index to build
		Set<AbstractIndexBuilder> indexBuilders = getSelectedIndexBuilders(client, entityDao, params);
		// Return the SinkManager
		Sink sink = new ElasticSearchWriterTask(indexAdminService, entityDao, indexBuilders);
		return new SinkManager(taskConfig.getId(), sink, taskConfig.getPipeArgs());
	}

	protected Properties getParameters(TaskConfiguration taskConfig) {
		Properties params = new Properties();
		params.put(PARAM_CLUSTER_NAME, getStringArgument(taskConfig, PARAM_CLUSTER_NAME,
				getDefaultStringArgument(taskConfig, "elasticsearch")));
		params.put(PARAM_HOSTS, getStringArgument(taskConfig, PARAM_HOSTS,
				getDefaultStringArgument(taskConfig, "localhost")));
		params.put(PARAM_INDEX_NAME, getStringArgument(taskConfig, PARAM_INDEX_NAME,
				getDefaultStringArgument(taskConfig, "osm")));
		params.put(PARAM_CREATE_INDEX, getBooleanArgument(taskConfig, PARAM_CREATE_INDEX, true));
		params.put(PARAM_INDEX_BUILDERS, getStringArgument(taskConfig, PARAM_INDEX_BUILDERS,
				getDefaultStringArgument(taskConfig, "")));
		return params;
	}

	protected Client buildElasticsearchClient(Properties params) {
		return ElasticSearchClientBuilder.newClient()
				.setClusterName((String) params.get(PARAM_CLUSTER_NAME))
				.setHosts((String) params.get(PARAM_HOSTS))
				.build();
	}

	protected EntityDao buildEntityDao(IndexAdminService indexAdminService, Properties params) {
		String indexName = (String) params.get(PARAM_INDEX_NAME);
		Boolean createIndex = (Boolean) params.get(PARAM_CREATE_INDEX);
		if (createIndex) indexAdminService.createIndex(indexName, new OsmIndexBuilder().getIndexMapping());
		EntityDao entityDao = new EntityDao(indexName, indexAdminService.getClient());
		return entityDao;
	}

	protected Set<AbstractIndexBuilder> getSelectedIndexBuilders(Client client, EntityDao entityDao, Properties params) {
		Properties properties = getIndexBuilderProperties();
		Set<AbstractIndexBuilder> set = new HashSet<AbstractIndexBuilder>();
		String indexName = (String) params.get(PARAM_INDEX_NAME);
		String selectedIndexBuilders = (String) params.get(PARAM_INDEX_BUILDERS);
		if (selectedIndexBuilders.isEmpty()) return set;
		for (String indexBuilderName : selectedIndexBuilders.split(",")) {
			if (!properties.containsKey(indexBuilderName)) {
				throw new RuntimeException("Unable to find IndexBuilder [" + indexBuilderName + "]");
			} else try {
				String indexBuilderClass = properties.getProperty(indexBuilderName);
				@SuppressWarnings("unchecked")
				Class<AbstractIndexBuilder> _class = (Class<AbstractIndexBuilder>) Class.forName(indexBuilderClass);
				Constructor<AbstractIndexBuilder> _const = _class.getDeclaredConstructor(Client.class, EntityDao.class, String.class);
				AbstractIndexBuilder indexBuilder = _const.newInstance(client, entityDao, indexName);
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
			InputStream inputStream = getClass().getClassLoader()
					.getResourceAsStream("indexBuilder.properties");
			properties.load(inputStream);
			return properties;
		} catch (Exception e) {
			throw new RuntimeException("Unable to load IndexBuilder list");
		}
	}

}
