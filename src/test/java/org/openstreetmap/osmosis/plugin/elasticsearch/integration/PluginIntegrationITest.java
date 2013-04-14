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
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESEntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.AbstractElasticSearchInMemoryTest;

public class PluginIntegrationITest extends AbstractElasticSearchInMemoryTest {

	private static final String INDEX_NAME = "osm-test";

	@Test
	public void countMainIndexedDocuments() throws Exception {
		// Action
		Osmosis.run(new String[] {
				"--read-xml",
				getResourceFile("mondeville-20130123.osm").getPath(),
				"--write-elasticsearch",
				"cluster.hosts=" + nodeAddress(),
				"cluster.name=" + clusterName(),
				"index.name=" + INDEX_NAME,
				"index.create=true"
		});
		refresh(INDEX_NAME);

		// Assert
		assertTrue(client().admin().indices().exists(new IndicesExistsRequest(INDEX_NAME)).actionGet().isExists());
		assertEquals(7738, client().count(new CountRequest(INDEX_NAME).types(ESEntityType.NODE.getIndiceName())).actionGet().getCount());
		assertEquals(225, client().count(new CountRequest(INDEX_NAME).types(ESEntityType.WAY.getIndiceName())).actionGet().getCount());
	}

	private File getResourceFile(String filename) throws URISyntaxException {
		URL url = getClass().getResource("/" + filename);
		return new File(url.toURI());
	}

}
