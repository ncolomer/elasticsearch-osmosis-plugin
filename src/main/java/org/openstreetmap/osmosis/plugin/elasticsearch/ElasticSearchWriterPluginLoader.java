package org.openstreetmap.osmosis.plugin.elasticsearch;

import java.util.HashMap;
import java.util.Map;

import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.plugin.PluginLoader;

public class ElasticSearchWriterPluginLoader implements PluginLoader {

	@Override
	public Map<String, TaskManagerFactory> loadTaskFactories() {
		ElasticSearchWriterFactory elasticSearchWriterFactory = new ElasticSearchWriterFactory();
		HashMap<String, TaskManagerFactory> map = new HashMap<String, TaskManagerFactory>();
		map.put("write-elasticsearch", elasticSearchWriterFactory);
		map.put("wes", elasticSearchWriterFactory);
		return map;
	}

}
