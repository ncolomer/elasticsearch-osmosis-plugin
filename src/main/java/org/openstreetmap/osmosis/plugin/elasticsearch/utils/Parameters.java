package org.openstreetmap.osmosis.plugin.elasticsearch.utils;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Parameters {

	public static final String PROPERTIES_FILE = "properties.file";

	public static final String CLUSTER_HOSTS = "cluster.hosts";
	public static final String CLUSTER_NAME = "cluster.name";

	public static final String INDEX_NAME = "index.name";
	public static final String INDEX_CREATE = "index.create";
	public static final String INDEX_SETTINGS_SHARDS = "index.settings.shards";
	public static final String INDEX_SETTINGS_REPLICAS = "index.settings.replicas";
	public static final String INDEX_MAPPING_NODE = "index.mapping.node";
	public static final String INDEX_MAPPING_WAY = "index.mapping.way";

	public static final String INDEX_BUILDERS = "index.builders";

	public static final String CONFIG_QUEUE_SIZE = "config.queue.size";
	public static final String CONFIG_NODE_BULK_SIZE = "config.node.bulk.size";
	public static final String CONFIG_WAY_BULK_SIZE = "config.way.bulk.size";
	public static final String CONFIG_WORKER_POOL_SIZE = "config.worker.pool.size";
        
        public static final String WITHOUT_WAYS = "without_ways";

	private final Properties params;

	private Parameters(Builder builder) {
		this.params = builder.params;
	}

	public String getProperty(String key) {
		return params.getProperty(key);
	}

	public String getProperty(String key, String defaultValue) {
		return params.getProperty(key, defaultValue);
	}

	public boolean containsKey(String key) {
		return params.containsKey(key);
	}

	@Override
	public String toString() {
		return "Parameters [params=" + params + "]";
	}

	public static class Builder {

		private Properties params = new Properties();

		public Builder loadResource(String resource) {
			try {
				params.load(getClass().getClassLoader().getResource(resource).openStream());
				return this;
			} catch (IOException e) {
				throw new RuntimeException("Unable to load properties resource " + resource);
			}
		}

		public Builder loadFile(String fileName) {
			try {
				params.load(new FileReader(fileName));
				return this;
			} catch (IOException e) {
				throw new RuntimeException("Unable to load properties file " + fileName);
			}
		}

		public Builder addParameter(String key, String value) {
			params.setProperty(key, value);
			return this;
		}

		public Parameters build() {
			return new Parameters(this);
		}

	}

}
