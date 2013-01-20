package org.openstreetmap.osmosis.plugin.elasticsearch.service;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.AbstractElasticSearchInMemoryTest;

public class IndexAdminServiceITest extends AbstractElasticSearchInMemoryTest {

	IndexAdminService indexAdminService;

	@Before
	public void setUp() {
		indexAdminService = new IndexAdminService(client());
	}

	@After
	public void tearDown() {
		delete();
		refresh();
	}

	@Test
	public void createIndex() {
		// Setup
		String indexName = "my_index";
		Map<String, XContentBuilder> mapping = getDummyMapping();

		// Action
		indexAdminService.createIndex(indexName, mapping);

		// Assert
		Assert.assertTrue(client().admin().indices().prepareExists(indexName)
				.execute().actionGet().exists());
	}

	@Test
	public void createIndex_withExistingIndex_shouldDelete() {
		// Setup
		String indexName = "my_index";
		client().admin().indices().prepareCreate(indexName).execute().actionGet();
		client().admin().indices().refresh(Requests.refreshRequest()).actionGet();
		Map<String, XContentBuilder> mapping = getDummyMapping();

		// Action
		indexAdminService.createIndex(indexName, mapping);

		// Assert
		Assert.assertTrue(client().admin().indices().prepareExists(indexName)
				.execute().actionGet().exists());
	}

	private Map<String, XContentBuilder> getDummyMapping() {
		try {
			Map<String, XContentBuilder> mapping = new HashMap<String, XContentBuilder>();
			XContentBuilder xContentBuilder = jsonBuilder()
					.startObject().startObject("my_type").startObject("properties")
					.endObject().endObject().endObject();
			mapping.put("my_type", xContentBuilder);
			return mapping;
		} catch (IOException e) {
			throw new IllegalStateException("Unable to create mapping", e);
		}
	}

}
