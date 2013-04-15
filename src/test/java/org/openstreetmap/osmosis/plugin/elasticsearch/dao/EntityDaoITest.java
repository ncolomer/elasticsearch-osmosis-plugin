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
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESEntity;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESEntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESWay;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.AbstractElasticSearchInMemoryTest;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.OsmDataBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Parameters;

public class EntityDaoITest extends AbstractElasticSearchInMemoryTest {

	private static final String INDEX_NAME = "osm-test";

	private EntityDao entityDao;

	@Before
	public void setUp() throws IOException {
		entityDao = new EntityDao(INDEX_NAME, client());
		Parameters params = new Parameters.Builder().loadResource("plugin.properties").build();
		IndexAdminService indexAdminService = new IndexAdminService(client());
		HashMap<String, String> mappings = new HashMap<String, String>();
		mappings.put(ESEntityType.NODE.getIndiceName(), params.getProperty(Parameters.INDEX_MAPPING_NODE));
		mappings.put(ESEntityType.WAY.getIndiceName(), params.getProperty(Parameters.INDEX_MAPPING_WAY));
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
		String expected = "{\"centroid\":[2.0,1.0],\"shape\":{\"type\":\"point\",\"coordinates\":[2.0,1.0]},\"tags\":{\"highway\":\"traffic_signals\"}}";
		String actual = response.getSourceAsString();
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void saveWay_withPolygon() {
		// Setup
		ESNode node1 = ESNode.Builder.create().id(1).location(1, 2).build();
		ESNode node2 = ESNode.Builder.create().id(2).location(2, 3).build();
		ESNode node3 = ESNode.Builder.create().id(3).location(3, 2).build();
		index(INDEX_NAME, node1, node2, node3);

		Way way = OsmDataBuilder.buildSampleWay(1, 1, 2, 3, 1);

		// Action
		entityDao.save(way);
		refresh(INDEX_NAME);

		// Assert
		GetResponse response = client().prepareGet(INDEX_NAME, "way", "1").execute().actionGet();
		Assert.assertTrue(response.isExists());
		String expected = "{\"centroid\":[2.3333333333333335,2.0],\"length\":536.8973391277414," +
				"\"area\":12364.345757132623,\"shape\":{\"type\":\"polygon\",\"coordinates\":" +
				"[[[2.0,1.0],[3.0,2.0],[2.0,3.0],[2.0,1.0]]]},\"tags\":{\"highway\":\"residential\"}}";
		String actual = response.getSourceAsString();
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void saveWay_withLineString() {
		// Setup
		ESNode node1 = ESNode.Builder.create().id(1).location(1.0, 2.0).build();
		ESNode node2 = ESNode.Builder.create().id(2).location(2.0, 3.0).build();
		ESNode node3 = ESNode.Builder.create().id(3).location(3.0, 2.0).build();
		ESNode node4 = ESNode.Builder.create().id(4).location(4.0, 1.0).build();
		index(INDEX_NAME, node1, node2, node3, node4);

		Way way = OsmDataBuilder.buildSampleWay(1, 1, 2, 3, 4);

		// Action
		entityDao.save(way);
		refresh(INDEX_NAME);

		// Assert
		GetResponse response = client().prepareGet(INDEX_NAME, "way", "1").execute().actionGet();
		Assert.assertTrue(response.isExists());
		String expected = "{\"centroid\":[2.1666666666666665,2.5],\"length\":471.76076948850596," +
				"\"area\":0.0,\"shape\":{\"type\":\"linestring\",\"coordinates\":" +
				"[[2.0,1.0],[3.0,2.0],[2.0,3.0],[1.0,4.0]]},\"tags\":{\"highway\":\"residential\"}}";
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
				"\"tags\":{\"highway\":\"traffic_signals\"}}";

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

	@Test
	public void findWay_withLineString() {
		// Setup
		ESWay way = ESWay.Builder.create().id(1).addLocation(1.0, 2.0).addLocation(2.0, 3.0)
				.addLocation(3.0, 2.0).addLocation(4.0, 1.0).build();
		index(INDEX_NAME, way);
		refresh(INDEX_NAME);

		// Action
		ESWay actual = entityDao.find(ESWay.class, 1);

		// Assert
		Assert.assertEquals(way, actual);
	}

	@Test
	public void findWay_withPolygon() {
		// Setup
		ESWay way = ESWay.Builder.create().id(1).addLocation(1.1, 2.1).addLocation(1.2, 2.2)
				.addLocation(1.3, 2.3).addLocation(1.1, 2.1).build();
		index(INDEX_NAME, way);
		refresh(INDEX_NAME);

		// Action
		ESWay actual = entityDao.find(ESWay.class, 1);

		// Assert
		Assert.assertEquals(way, actual);
	}

	@Test(expected = DaoException.class)
	public void findWay_thatDoesNotExists() {
		// Setup
		ESWay way = ESWay.Builder.create().id(1).addLocation(1.1, 2.1).addLocation(1.2, 2.2)
				.addLocation(1.3, 2.3).addLocation(1.4, 2.4).build();
		index(INDEX_NAME, way);
		refresh(INDEX_NAME);

		// Action
		entityDao.find(ESWay.class, 2);
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

	@Test
	public void findAllWays() {
		// Setup
		// Setup
		ESWay way1 = ESWay.Builder.create().id(1).addLocation(1.1, 2.1).addLocation(1.2, 2.2)
				.addLocation(1.3, 2.3).addLocation(1.4, 2.4).build();
		ESWay way2 = ESWay.Builder.create().id(2).addLocation(1.1, 2.1).addLocation(1.2, 2.2)
				.addLocation(1.3, 2.3).addLocation(1.4, 2.4).build();
		List<ESWay> expected = Arrays.asList(new ESWay[] { way1, way2 });

		index(INDEX_NAME, way1, way2);
		refresh(INDEX_NAME);

		// Action
		List<ESWay> actual = entityDao.findAll(ESWay.class, 1l, 2);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void findAllWays_withSubset() {
		// Setup
		ESWay way1 = ESWay.Builder.create().id(1).addLocation(1.1, 2.1).addLocation(1.2, 2.2)
				.addLocation(1.3, 2.3).addLocation(1.4, 2.4).build();
		ESWay way2 = ESWay.Builder.create().id(2).addLocation(1.1, 2.1).addLocation(1.2, 2.2)
				.addLocation(1.3, 2.3).addLocation(1.4, 2.4).build();
		List<ESWay> expected = Arrays.asList(new ESWay[] { way2 });

		index(INDEX_NAME, way1, way2);
		refresh(INDEX_NAME);

		// Action
		List<ESWay> actual = entityDao.findAll(ESWay.class, 2);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void findAllWays_keepOrder() {
		// Setup
		ESWay way1 = ESWay.Builder.create().id(1).addLocation(1.1, 2.1).addLocation(1.2, 2.2)
				.addLocation(1.3, 2.3).addLocation(1.4, 2.4).build();
		ESWay way2 = ESWay.Builder.create().id(2).addLocation(1.1, 2.1).addLocation(1.2, 2.2)
				.addLocation(1.3, 2.3).addLocation(1.4, 2.4).build();
		List<ESWay> expected = Arrays.asList(new ESWay[] { way2, way1 });

		index(INDEX_NAME, way1, way2);
		refresh(INDEX_NAME);

		// Action
		List<ESWay> actual = entityDao.findAll(ESWay.class, 2l, 1);

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

	@Test
	public void deleteWay() {
		// Setup
		ESWay way = ESWay.Builder.create().id(1).addLocation(1.1, 2.1).addLocation(1.2, 2.2)
				.addLocation(1.3, 2.3).addLocation(1.4, 2.4).build();
		index(INDEX_NAME, way);
		refresh(INDEX_NAME);

		// Action
		boolean actual = entityDao.delete(ESWay.class, 1);

		// Assert
		Assert.assertTrue(actual);
	}

	@Test
	public void deleteWay_thatDoesNotExists() {
		// Setup
		ESWay way = ESWay.Builder.create().id(1).addLocation(1.1, 2.1).addLocation(1.2, 2.2)
				.addLocation(1.3, 2.3).addLocation(1.4, 2.4).build();
		index(INDEX_NAME, way);
		refresh(INDEX_NAME);

		// Action
		boolean actual = entityDao.delete(ESWay.class, 2);

		// Assert
		Assert.assertFalse(actual);
	}

}
