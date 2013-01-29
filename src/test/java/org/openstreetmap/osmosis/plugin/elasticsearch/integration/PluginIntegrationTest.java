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
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.AbstractElasticSearchInMemoryTest;

public class PluginIntegrationTest extends AbstractElasticSearchInMemoryTest {

	private static final String INDEX_NAME = "osm-test";

	@Test
	public void countMainIndexedDocuments() throws Exception {
		// Action
		Osmosis.run(new String[] {
				"--read-xml",
				getOsmExtractFile().getPath(),
				"--write-elasticsearch",
				"hosts=" + nodeAddress(),
				"clusterName=" + clusterName(),
				"indexName=" + INDEX_NAME,
				"createIndex=true"
		});

		// Assert
		assertTrue(client().admin().indices().exists(new IndicesExistsRequest(INDEX_NAME)).actionGet().exists());
		assertEquals(7738, client().count(new CountRequest(INDEX_NAME).types(EntityDao.NODE)).actionGet().count());
		assertEquals(225, client().count(new CountRequest(INDEX_NAME).types(EntityDao.WAY)).actionGet().count());
	}

	@Test
	public void countRgIndexedDocuments() throws Exception {
		// Action
		Osmosis.run(new String[] {
				"--read-xml",
				getOsmExtractFile().getPath(),
				"--write-elasticsearch",
				"hosts=" + nodeAddress(),
				"clusterName=" + clusterName(),
				"indexName=" + INDEX_NAME,
				"createIndex=true",
				"indexBuilders=rg"
		});

		// Assert
		String RG_INDEX_NAME = INDEX_NAME + "-rg";
		assertTrue(client().admin().indices().exists(new IndicesExistsRequest(RG_INDEX_NAME)).actionGet().exists());
		assertEquals(57, client().count(new CountRequest(RG_INDEX_NAME).types(EntityDao.WAY)).actionGet().count());
	}

	private File getOsmExtractFile() throws URISyntaxException {
		URL url = this.getClass().getResource("/mondeville-20130123.osm");
		return new File(url.toURI());
	}

}
