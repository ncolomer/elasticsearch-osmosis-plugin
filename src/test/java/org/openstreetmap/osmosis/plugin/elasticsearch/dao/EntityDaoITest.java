package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.elasticsearch.action.get.GetResponse;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.AbstractElasticSearchInMemoryTest;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.AssertUtils;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.OsmDataBuilder;

public class EntityDaoITest extends AbstractElasticSearchInMemoryTest {

	private static final String INDEX_NAME = "osm-test";

	private EntityDao entityDao;

	@Before
	public void setUp() throws IOException {
		Properties params = new Properties();
		params.load(getClass().getClassLoader().getResourceAsStream("plugin.properties"));
		IndexAdminService indexService = new IndexAdminService(client());
		indexService.createIndex(INDEX_NAME, (String) params.get("index.config"));
		// Create tested objects
		entityDao = new EntityDao(INDEX_NAME, client());
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
		Assert.assertTrue(response.exists());
		String expected = "{\"location\":[2.0,1.0],\"tags\":{\"highway\":\"traffic_signals\"}}";
		String actual = response.getSourceAsString();
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void saveWay() {
		// Setup
		Way way = OsmDataBuilder.buildSampleWay();

		// Action
		entityDao.save(way);
		refresh(INDEX_NAME);

		// Assert
		GetResponse response = client().prepareGet(INDEX_NAME, "way", "1").execute().actionGet();
		Assert.assertTrue(response.exists());
		String expected = "{\"tags\":{\"highway\":\"residential\"},\"nodes\":[1]}";
		String actual = response.getSourceAsString();
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void saveAll() throws InterruptedException {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();
		Way way = OsmDataBuilder.buildSampleWay();

		// Action
		entityDao.saveAll(Arrays.asList(new Entity[] { node, way }));
		refresh(INDEX_NAME);

		// Assert
		GetResponse response1 = client().prepareGet(INDEX_NAME, "node", "1").execute().actionGet();
		Assert.assertTrue(response1.exists());
		String expected1 = "{\"location\":[2.0,1.0],\"tags\":{\"highway\":\"traffic_signals\"}}";
		String actual1 = response1.getSourceAsString();
		Assert.assertEquals(expected1, actual1);

		GetResponse response2 = client().prepareGet(INDEX_NAME, "way", "1").execute().actionGet();
		Assert.assertTrue(response2.exists());
		String expected2 = "{\"tags\":{\"highway\":\"residential\"},\"nodes\":[1]}";
		String actual2 = response2.getSourceAsString();
		Assert.assertEquals(expected2, actual2);
	}

	/* find */

	@Test
	public void findNode() throws Exception {
		// Setup
		Node expected = OsmDataBuilder.buildSampleNode();
		client().prepareIndex(INDEX_NAME, "node", "1")
				.setSource(new EntityMapper().marshall(expected))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		Node actual = entityDao.find(Node.class, 1l);

		// Assert
		AssertUtils.assertEquals(expected, actual);
	}

	@Test
	public void findNode_thatDoesNotExists() throws Exception {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();
		client().prepareIndex(INDEX_NAME, "node", "1")
				.setSource(new EntityMapper().marshall(node))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		Node actual = entityDao.find(Node.class, 2l);

		// Assert
		Assert.assertNull(actual);
	}

	@Test
	public void findWay() throws Exception {
		// Setup
		Way way = OsmDataBuilder.buildSampleWay();
		client().prepareIndex(INDEX_NAME, "way", "1")
				.setSource(new EntityMapper().marshall(way))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		Way actual = entityDao.find(Way.class, 1l);

		// Assert
		Assert.assertEquals(1l, actual.getId());
		Tag tag = actual.getTags().iterator().next();
		Assert.assertEquals("highway", tag.getKey());
		Assert.assertEquals("residential", tag.getValue());
		WayNode wayNode = actual.getWayNodes().get(0);
		Assert.assertEquals(1l, wayNode.getNodeId());
	}

	@Test
	public void findWay_thatDoesNotExists() throws Exception {
		// Setup
		Way way = OsmDataBuilder.buildSampleWay();
		client().prepareIndex(INDEX_NAME, "way", "1")
				.setSource(new EntityMapper().marshall(way))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		Way actual = entityDao.find(Way.class, 2l);

		// Assert
		Assert.assertNull(actual);
	}

	/* findAll */

	@Test
	public void findAllNodes_stressTest() throws Exception {
		// Setup

		int SIZE = 100;

		long[] ids = new long[SIZE];
		List<Node> expected = new ArrayList<Node>(SIZE);

		for (int i = 0; i < SIZE; i++) {
			Node node = OsmDataBuilder.buildSampleNode();
			node.setId(i);
			expected.add(node);
			ids[i] = i;
			client().prepareIndex(INDEX_NAME, "node", String.valueOf(i))
					.setSource(new EntityMapper().marshall(node))
					.execute().actionGet();
		}
		refresh(INDEX_NAME);

		// Action
		List<Node> actual = entityDao.findAll(Node.class, ids);

		// Assert
		AssertUtils.assertNodesEquals(expected, actual);
	}

	@Test
	public void findAllNodes() throws Exception {
		// Setup
		Node node1 = OsmDataBuilder.buildSampleNode();
		node1.setId(1);
		Node node2 = OsmDataBuilder.buildSampleNode();
		node2.setId(2);
		List<Node> expected = Arrays.asList(new Node[] { node1, node2 });

		client().prepareIndex(INDEX_NAME, "node", "1")
				.setSource(new EntityMapper().marshall(node1))
				.execute().actionGet();
		client().prepareIndex(INDEX_NAME, "node", "2")
				.setSource(new EntityMapper().marshall(node2))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		List<Node> actual = entityDao.findAll(Node.class, 1l, 2l);

		// Assert
		AssertUtils.assertNodesEquals(expected, actual);
	}

	@Test
	public void findAllNodes_withSubset() throws Exception {
		// Setup
		Node node1 = OsmDataBuilder.buildSampleNode();
		node1.setId(1);
		Node node2 = OsmDataBuilder.buildSampleNode();
		node2.setId(2);
		List<Node> expected = Arrays.asList(new Node[] { node2 });

		client().prepareIndex(INDEX_NAME, "node", "1")
				.setSource(new EntityMapper().marshall(node1))
				.execute().actionGet();
		client().prepareIndex(INDEX_NAME, "node", "2")
				.setSource(new EntityMapper().marshall(node2))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		List<Node> actual = entityDao.findAll(Node.class, 2l);

		// Assert
		AssertUtils.assertNodesEquals(expected, actual);
	}

	@Test
	public void findAllNodes_keepOrder() throws Exception {
		// Setup
		Node node1 = OsmDataBuilder.buildSampleNode();
		node1.setId(1);
		Node node2 = OsmDataBuilder.buildSampleNode();
		node2.setId(2);
		List<Node> expected = Arrays.asList(new Node[] { node2, node1 });

		client().prepareIndex(INDEX_NAME, "node", "1")
				.setSource(new EntityMapper().marshall(node1))
				.execute().actionGet();
		client().prepareIndex(INDEX_NAME, "node", "2")
				.setSource(new EntityMapper().marshall(node2))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		List<Node> actual = entityDao.findAll(Node.class, 2l, 1l);

		// Assert
		AssertUtils.assertNodesEquals(expected, actual);
	}

	@Test
	public void findAllWays() throws Exception {
		// Setup
		Way way1 = OsmDataBuilder.buildSampleWay();
		way1.setId(1);
		Way way2 = OsmDataBuilder.buildSampleWay();
		way2.setId(2);
		List<Way> expected = Arrays.asList(new Way[] { way1, way2 });

		client().prepareIndex(INDEX_NAME, "way", "1")
				.setSource(new EntityMapper().marshall(way1))
				.execute().actionGet();
		client().prepareIndex(INDEX_NAME, "way", "2")
				.setSource(new EntityMapper().marshall(way2))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		List<Way> actual = entityDao.findAll(Way.class, 1l, 2l);

		// Assert
		AssertUtils.assertWaysEquals(expected, actual);
	}

	@Test
	public void findAllWays_withSubset() throws Exception {
		// Setup
		Way way1 = OsmDataBuilder.buildSampleWay();
		way1.setId(1);
		Way way2 = OsmDataBuilder.buildSampleWay();
		way2.setId(2);
		List<Way> expected = Arrays.asList(new Way[] { way2 });

		client().prepareIndex(INDEX_NAME, "way", "1")
				.setSource(new EntityMapper().marshall(way1))
				.execute().actionGet();
		client().prepareIndex(INDEX_NAME, "way", "2")
				.setSource(new EntityMapper().marshall(way2))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		List<Way> actual = entityDao.findAll(Way.class, 2l);

		// Assert
		AssertUtils.assertWaysEquals(expected, actual);
	}

	@Test
	public void findAllWays_keepOrder() throws Exception {
		// Setup
		Way way1 = OsmDataBuilder.buildSampleWay();
		way1.setId(1);
		Way way2 = OsmDataBuilder.buildSampleWay();
		way2.setId(2);
		List<Way> expected = Arrays.asList(new Way[] { way2, way1 });

		client().prepareIndex(INDEX_NAME, "way", "1")
				.setSource(new EntityMapper().marshall(way1))
				.execute().actionGet();
		client().prepareIndex(INDEX_NAME, "way", "2")
				.setSource(new EntityMapper().marshall(way2))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		List<Way> actual = entityDao.findAll(Way.class, 2l, 1l);

		// Assert
		AssertUtils.assertWaysEquals(expected, actual);
	}

	/* delete */

	@Test
	public void deleteNode() throws Exception {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();
		client().prepareIndex(INDEX_NAME, "node", "1")
				.setSource(new EntityMapper().marshall(node))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		boolean actual = entityDao.delete(Node.class, 1l);

		// Assert
		Assert.assertTrue(actual);
	}

	@Test
	public void deleteNode_thatDoesNotExists() throws Exception {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();
		client().prepareIndex(INDEX_NAME, "node", "1")
				.setSource(new EntityMapper().marshall(node))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		boolean actual = entityDao.delete(Node.class, 2l);

		// Assert
		Assert.assertFalse(actual);
	}

	@Test
	public void deleteWay() throws Exception {
		// Setup
		Way way = OsmDataBuilder.buildSampleWay();
		client().prepareIndex(INDEX_NAME, "way", "1")
				.setSource(new EntityMapper().marshall(way))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		boolean actual = entityDao.delete(Way.class, 1l);

		// Assert
		Assert.assertTrue(actual);
	}

	@Test
	public void deleteWay_thatDoesNotExists() throws Exception {
		// Setup
		Way way = OsmDataBuilder.buildSampleWay();
		client().prepareIndex(INDEX_NAME, "way", "1")
				.setSource(new EntityMapper().marshall(way))
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		boolean actual = entityDao.delete(Way.class, 2l);

		// Assert
		Assert.assertFalse(actual);
	}

}
