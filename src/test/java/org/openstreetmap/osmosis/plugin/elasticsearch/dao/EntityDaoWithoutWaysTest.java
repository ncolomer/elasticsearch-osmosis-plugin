package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.elasticsearch.action.get.GetResponse;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESEntity;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESEntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.AbstractElasticSearchInMemoryTest;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.OsmDataBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Parameters;

public class EntityDaoWithoutWaysTest extends AbstractElasticSearchInMemoryTest {

	private static final String INDEX_NAME = "osm-test";

        private static final Boolean WITHOUT_WAYS = true;
        
	private EntityDao entityDao;

	@Before
	public void setUp() throws IOException {
		entityDao = new EntityDao(INDEX_NAME, client(), WITHOUT_WAYS);
		Parameters params = new Parameters.Builder().loadResource("plugin.properties").build();
		IndexAdminService indexAdminService = new IndexAdminService(client());
		HashMap<String, String> mappings = new HashMap<String, String>();
		mappings.put(ESEntityType.NODE.getIndiceName(), params.getProperty(Parameters.INDEX_MAPPING_NODE));
		indexAdminService.createIndex(INDEX_NAME, 1, 0, mappings);
	}

	/* save */

	@Test
	public void saveNode() {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();

		// Action
		entityDao.save(node);
		refresh(INDEX_NAME);

		// Assert
		GetResponse response = client().prepareGet(INDEX_NAME, "node", "1").execute().actionGet();
		Assert.assertTrue(response.isExists());
		String expected = "{\"centroid\":[2.0,1.0],\"shape\":{\"type\":\"point\",\"coordinates\":[2.0,1.0]},\"tags\":{\"place\":\"town\"}}";
		String actual = response.getSourceAsString();
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void saveAll() throws InterruptedException {
		// Setup
		Node node1 = OsmDataBuilder.buildSampleNode(1);
		Node node2 = OsmDataBuilder.buildSampleNode(2);

		// Action
		entityDao.saveAll(Arrays.asList(new Entity[] { node1, node2 }));
		refresh(INDEX_NAME);

		// Assert
		String expected = "{\"centroid\":[2.0,1.0],\"shape\":{\"type\":\"point\",\"coordinates\":[2.0,1.0]}," +
				"\"tags\":{\"place\":\"town\"}}";

		GetResponse response1 = client().prepareGet(INDEX_NAME, "node", "1").execute().actionGet();
		Assert.assertTrue(response1.isExists());
		String actual1 = response1.getSourceAsString();
		Assert.assertEquals(expected, actual1);

		GetResponse response2 = client().prepareGet(INDEX_NAME, "node", "2").execute().actionGet();
		Assert.assertTrue(response2.isExists());
		String actual2 = response2.getSourceAsString();
		Assert.assertEquals(expected, actual2);
	}

	/* find */

	@Test
	public void findNode() {
		// Setup
		ESNode node = OsmDataBuilder.buildSampleESNode();
		index(INDEX_NAME, node);
		refresh(INDEX_NAME);

		// Action
		ESNode actual = entityDao.find(ESNode.class, 1);

		// Assert
		Assert.assertEquals(node, actual);
	}

	@Test(expected = DaoException.class)
	public void findNode_thatDoesNotExists() {
		// Setup
		ESNode node = OsmDataBuilder.buildSampleESNode();
		index(INDEX_NAME, node);
		refresh(INDEX_NAME);

		// Action
		entityDao.find(ESNode.class, 2);
	}

        /* findAll */

	@Test
	public void findAllNodes_stressTest() {
		// Setup
		int SIZE = 100;

		long[] ids = new long[SIZE];
		List<ESNode> expected = new ArrayList<ESNode>(SIZE);

		for (int i = 0; i < SIZE; i++) {
			ESNode node = OsmDataBuilder.buildSampleESNode(i);
			expected.add(node);
			ids[i] = i;

		}
		index(INDEX_NAME, expected.toArray(new ESEntity[0]));
		refresh(INDEX_NAME);

		// Action
		List<ESNode> actual = entityDao.findAll(ESNode.class, ids);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void findAllNodes() {
		// Setup
		ESNode node1 = ESNode.Builder.create().id(1).location(1.0, 2.0).build();
		ESNode node2 = ESNode.Builder.create().id(2).location(3.0, 4.0).build();
		List<ESNode> expected = Arrays.asList(new ESNode[] { node1, node2 });

		index(INDEX_NAME, node1, node2);
		refresh(INDEX_NAME);

		// Action
		List<ESNode> actual = entityDao.findAll(ESNode.class, 1l, 2);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void findAllNodes_withSubset() {
		// Setup
		ESNode node1 = ESNode.Builder.create().id(1).location(1.0, 2.0).build();
		ESNode node2 = ESNode.Builder.create().id(2).location(3.0, 4.0).build();
		List<ESNode> expected = Arrays.asList(new ESNode[] { node2 });

		index(INDEX_NAME, node1, node2);
		refresh(INDEX_NAME);

		// Action
		List<ESNode> actual = entityDao.findAll(ESNode.class, 2);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void findAllNodes_keepOrder() {
		// Setup
		ESNode node1 = ESNode.Builder.create().id(1).location(1.0, 2.0).build();
		ESNode node2 = ESNode.Builder.create().id(2).location(3.0, 4.0).build();
		List<ESNode> expected = Arrays.asList(new ESNode[] { node2, node1 });

		index(INDEX_NAME, node1, node2);
		refresh(INDEX_NAME);

		// Action
		List<ESNode> actual = entityDao.findAll(ESNode.class, 2l, 1);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	/* delete */

	@Test
	public void deleteNode() {
		// Setup
		ESNode node = OsmDataBuilder.buildSampleESNode();
		index(INDEX_NAME, node);
		refresh(INDEX_NAME);

		// Action
		boolean actual = entityDao.delete(ESNode.class, 1);

		// Assert
		Assert.assertTrue(actual);
	}

	@Test
	public void deleteNode_thatDoesNotExists() {
		// Setup
		ESNode node = OsmDataBuilder.buildSampleESNode();
		index(INDEX_NAME, node);
		refresh(INDEX_NAME);

		// Action
		boolean actual = entityDao.delete(ESNode.class, 2);

		// Assert
		Assert.assertFalse(actual);
	}

}
