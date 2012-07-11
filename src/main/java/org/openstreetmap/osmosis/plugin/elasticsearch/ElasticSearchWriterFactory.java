package org.openstreetmap.osmosis.plugin.elasticsearch;

import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.SinkManager;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.ElasticSearchClientBuilder;

public class ElasticSearchWriterFactory extends TaskManagerFactory {

	public static final String PARAM_HOST = "host";
	public static final String PARAM_PORT = "port";
	public static final String PARAM_CLUSTERNAME = "clusterName";

	@Override
	protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {
		ElasticSearchClientBuilder builder = new ElasticSearchClientBuilder();

		builder.host = getStringArgument(taskConfig, PARAM_HOST,
				getDefaultStringArgument(taskConfig, "localhost"));

		builder.port = getIntegerArgument(taskConfig, PARAM_PORT,
				getDefaultIntegerArgument(taskConfig, 9300));

		builder.clusterName = getStringArgument(taskConfig, PARAM_CLUSTERNAME,
				getDefaultStringArgument(taskConfig, "elasticsearch"));

		Sink sink = new ElasticSearchWriterTask(builder.build());
		return new SinkManager(taskConfig.getId(), sink, taskConfig.getPipeArgs());
	}

}
