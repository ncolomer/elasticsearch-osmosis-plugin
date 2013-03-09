package org.openstreetmap.osmosis.plugin.elasticsearch.utils;

import junit.framework.Assert;

import org.junit.Test;

public class ParametersUTest {

	@Test
	public void loadResource() {
		// Action
		Parameters p = new Parameters.Builder().loadResource("plugin.properties").build();

		// Assert
		Assert.assertTrue(p.containsKey(Parameters.PARAM_CLUSTER_HOSTS));
		Assert.assertEquals("localhost", p.getProperty(Parameters.PARAM_CLUSTER_HOSTS));
		Assert.assertEquals("localhost", p.getProperty(Parameters.PARAM_CLUSTER_HOSTS, "default"));
		Assert.assertNull(p.getProperty("DUMMY_PROPERTY"));
		Assert.assertEquals("default", p.getProperty("DUMMY_PROPERTY", "default"));
	}

	@Test
	public void loadFile() {
		// Setup
		String file = getClass().getClassLoader().getResource("plugin.properties").getPath();

		// Action
		Parameters p = new Parameters.Builder().loadFile(file).build();

		// Assert
		Assert.assertTrue(p.containsKey(Parameters.PARAM_CLUSTER_HOSTS));
		Assert.assertEquals("localhost", p.getProperty(Parameters.PARAM_CLUSTER_HOSTS));
		Assert.assertEquals("localhost", p.getProperty(Parameters.PARAM_CLUSTER_HOSTS, "default"));
		Assert.assertNull(p.getProperty("DUMMY_PROPERTY"));
		Assert.assertEquals("default", p.getProperty("DUMMY_PROPERTY", "default"));
	}

	@Test
	public void addParameter() {
		// Action
		Parameters p = new Parameters.Builder().loadResource("plugin.properties")
				.addParameter(Parameters.PARAM_CLUSTER_HOSTS, "192.168.1.1").build();

		// Assert
		Assert.assertTrue(p.containsKey(Parameters.PARAM_CLUSTER_HOSTS));
		Assert.assertEquals("192.168.1.1", p.getProperty(Parameters.PARAM_CLUSTER_HOSTS));
		Assert.assertEquals("192.168.1.1", p.getProperty(Parameters.PARAM_CLUSTER_HOSTS, "default"));
		Assert.assertNull(p.getProperty("DUMMY_PROPERTY"));
		Assert.assertEquals("default", p.getProperty("DUMMY_PROPERTY", "default"));
	}

}
