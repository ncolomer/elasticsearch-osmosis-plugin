package org.openstreetmap.osmosis.plugin.elasticsearch.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.count.CountRequest;
import org.junit.Test;
import org.openstreetmap.osmosis.core.Osmosis;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.ESEntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.AbstractElasticSearchInMemoryTest;

public class PluginIntegrationTest extends AbstractElasticSearchInMemoryTest {

	private static final String INDEX_NAME = "osm-test";

	@Test
	public void countMainIndexedDocuments() throws Exception {
		// Action
		Osmosis.run(new String[] {
				"--read-xml",
				getOsmExtractFile().getPath(),
				"--write-elasticsearch",
				"cluster.hosts=" + nodeAddress(),
				"cluster.name=" + clusterName(),
				"index.name=" + INDEX_NAME,
				"index.create=true"
		});

		// Assert
		assertTrue(client().admin().indices().exists(new IndicesExistsRequest(INDEX_NAME)).actionGet().exists());
		assertEquals(7738, client().count(new CountRequest(INDEX_NAME).types(ESEntityType.NODE.getIndiceName())).actionGet().count());
		assertEquals(225, client().count(new CountRequest(INDEX_NAME).types(ESEntityType.WAY.getIndiceName())).actionGet().count());
	}

	private File getOsmExtractFile() throws URISyntaxException {
		URL url = getClass().getResource("/mondeville-20130123.osm");
		return new File(url.toURI());
	}

}
