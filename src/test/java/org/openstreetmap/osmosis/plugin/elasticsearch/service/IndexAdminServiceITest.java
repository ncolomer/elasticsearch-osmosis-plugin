package org.openstreetmap.osmosis.plugin.elasticsearch.service;

import java.io.IOException;

import junit.framework.Assert;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.AbstractElasticSearchInMemoryTest;

public class IndexAdminServiceITest extends AbstractElasticSearchInMemoryTest {

	private static final String INDEX_NAME = "osm-test";

	private IndexAdminService indexAdminService;

	@Before
	public void setUp() {
		indexAdminService = new IndexAdminService(client());
	}

	@Test
	public void createIndex() throws IOException {
		// Action
		indexAdminService.createIndex(INDEX_NAME, 1, 0, "{}");

		// Assert
		Assert.assertTrue(exists(INDEX_NAME));
	}

	@Test
	public void createIndex_withExistingIndex_shouldDelete() {
		// Setup
		client().admin().indices().prepareCreate(INDEX_NAME).execute().actionGet();
		refresh(INDEX_NAME);
		Assume.assumeTrue(exists(INDEX_NAME));

		// Action
		indexAdminService.createIndex(INDEX_NAME, 1, 0, "{}");

		// Assert
		Assert.assertTrue(exists(INDEX_NAME));
	}

	@Test
	public void createIndex_withSettingsAndMappings() throws IOException {
		// Setup
		String mapping = XContentFactory.jsonBuilder()
				.startObject().startObject("properties")
				.startObject("my_field").field("type", "string").endObject()
				.endObject().endObject().string();

		// Action
		indexAdminService.createIndex(INDEX_NAME, 1, 1, mapping);

		// Assert
		ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
		Assert.assertEquals(1, state.getMetaData().index(INDEX_NAME).getNumberOfShards());
		Assert.assertEquals(1, state.getMetaData().index(INDEX_NAME).getNumberOfReplicas());
		Assert.assertEquals("{\"node\":{\"properties\":{\"my_field\":{\"type\":\"string\"}}}}",
				state.getMetaData().index(INDEX_NAME).mapping("node").source().string());
		Assert.assertEquals("{\"way\":{\"properties\":{\"my_field\":{\"type\":\"string\"}}}}",
				state.getMetaData().index(INDEX_NAME).mapping("way").source().string());
	}

}
