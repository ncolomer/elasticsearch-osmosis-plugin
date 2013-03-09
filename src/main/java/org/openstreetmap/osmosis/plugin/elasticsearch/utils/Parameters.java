package org.openstreetmap.osmosis.plugin.elasticsearch.utils;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Parameters {

	public static final String PARAM_PROPERTIES_FILE = "properties.file";
	public static final String PARAM_CLUSTER_HOSTS = "cluster.hosts";
	public static final String PARAM_CLUSTER_NAME = "cluster.name";
	public static final String PARAM_INDEX_NAME = "index.name";
	public static final String PARAM_INDEX_CREATE = "index.create";
	public static final String PARAM_INDEX_CONFIG = "index.config";
	public static final String PARAM_INDEX_BUILDERS = "index.builders";

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
