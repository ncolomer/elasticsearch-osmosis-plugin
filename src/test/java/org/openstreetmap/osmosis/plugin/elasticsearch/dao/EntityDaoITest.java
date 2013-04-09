package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.elasticsearch.action.get.GetResponse;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.ESNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.ESWay;
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
		indexAdminService.createIndex(INDEX_NAME, 1, 0, params.getProperty(Parameters.INDEX_MAPPINGS));
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
		String expected = "{\"shape\":{\"type\":\"point\",\"coordinates\":[2.0,1.0]},\"tags\":{\"highway\":\"traffic_signals\"}}";
		String actual = response.getSourceAsString();
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void saveWay() {
		// Setup
		ESNode node = ESNode.Builder.create().id(1).location(1.0d, 2.0d).build();
		client().prepareIndex(INDEX_NAME, node.getType().getIndiceName(), node.getIdString())
				.setSource(node.toJson())
				.execute().actionGet();

		Way way = OsmDataBuilder.buildSampleWay();

		// Action
		entityDao.save(way);
		refresh(INDEX_NAME);

		// Assert
		GetResponse response = client().prepareGet(INDEX_NAME, "way", "1").execute().actionGet();
		Assert.assertTrue(response.isExists());
		String expected = "{\"shape\":{\"type\":\"polygon\",\"coordinates\":[[[2.0,1.0]]]},\"tags\":{\"highway\":\"residential\"}}";
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
		GetResponse response1 = client().prepareGet(INDEX_NAME, "node", "1").execute().actionGet();
		Assert.assertTrue(response1.isExists());
		String expected1 = "{\"shape\":{\"type\":\"point\",\"coordinates\":[2.0,1.0]},\"tags\":{\"highway\":\"traffic_signals\"}}";
		String actual1 = response1.getSourceAsString();
		Assert.assertEquals(expected1, actual1);

		GetResponse response2 = client().prepareGet(INDEX_NAME, "node", "2").execute().actionGet();
		Assert.assertTrue(response2.isExists());
		String expected2 = "{\"shape\":{\"type\":\"point\",\"coordinates\":[2.0,1.0]},\"tags\":{\"highway\":\"traffic_signals\"}}";
		String actual2 = response2.getSourceAsString();
		Assert.assertEquals(expected2, actual2);
	}

	/* find */

	@Test
	public void findNode() {
		// Setup
		ESNode expected = OsmDataBuilder.buildSampleESNode();
		client().prepareIndex(INDEX_NAME, expected.getType().getIndiceName(), expected.getIdString())
				.setSource(expected.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		ESNode actual = entityDao.find(ESNode.class, 1l);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	@Test(expected = DaoException.class)
	public void findNode_thatDoesNotExists() {
		// Setup
		ESNode node = OsmDataBuilder.buildSampleESNode();
		client().prepareIndex(INDEX_NAME, node.getType().getIndiceName(), node.getIdString())
				.setSource(node.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		entityDao.find(ESNode.class, 2l);
	}

	@Test
	public void findWay() {
		// Setup
		ESWay expected = OsmDataBuilder.buildSampleESWay();
		client().prepareIndex(INDEX_NAME, expected.getType().getIndiceName(), expected.getIdString())
				.setSource(expected.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		ESWay actual = entityDao.find(ESWay.class, 1l);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	@Test(expected = DaoException.class)
	public void findWay_thatDoesNotExists() {
		// Setup
		ESWay way = OsmDataBuilder.buildSampleESWay();
		client().prepareIndex(INDEX_NAME, way.getType().getIndiceName(), way.getIdString())
				.setSource(way.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		entityDao.find(ESWay.class, 2l);
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
			client().prepareIndex(INDEX_NAME, node.getType().getIndiceName(), node.getIdString())
					.setSource(node.toJson())
					.execute().actionGet();
		}
		refresh(INDEX_NAME);

		// Action
		List<ESNode> actual = entityDao.findAll(ESNode.class, ids);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void findAllNodes() {
		// Setup
		ESNode node1 = ESNode.Builder.create().id(1).location(1.0d, 2.0d).build();
		ESNode node2 = ESNode.Builder.create().id(2).location(3.0d, 4.0d).build();
		List<ESNode> expected = Arrays.asList(new ESNode[] { node1, node2 });

		client().prepareIndex(INDEX_NAME, node1.getType().getIndiceName(), node1.getIdString())
				.setSource(node1.toJson())
				.execute().actionGet();
		client().prepareIndex(INDEX_NAME, node2.getType().getIndiceName(), node2.getIdString())
				.setSource(node2.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		List<ESNode> actual = entityDao.findAll(ESNode.class, 1l, 2l);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void findAllNodes_withSubset() {
		// Setup
		ESNode node1 = ESNode.Builder.create().id(1).location(1.0d, 2.0d).build();
		ESNode node2 = ESNode.Builder.create().id(2).location(3.0d, 4.0d).build();
		List<ESNode> expected = Arrays.asList(new ESNode[] { node2 });

		client().prepareIndex(INDEX_NAME, node1.getType().getIndiceName(), node1.getIdString())
				.setSource(node1.toJson())
				.execute().actionGet();
		client().prepareIndex(INDEX_NAME, node2.getType().getIndiceName(), node2.getIdString())
				.setSource(node2.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		List<ESNode> actual = entityDao.findAll(ESNode.class, 2l);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void findAllNodes_keepOrder() {
		// Setup
		ESNode node1 = ESNode.Builder.create().id(1).location(1.0d, 2.0d).build();
		ESNode node2 = ESNode.Builder.create().id(2).location(3.0d, 4.0d).build();
		List<ESNode> expected = Arrays.asList(new ESNode[] { node2, node1 });

		client().prepareIndex(INDEX_NAME, node1.getType().getIndiceName(), node1.getIdString())
				.setSource(node1.toJson())
				.execute().actionGet();
		client().prepareIndex(INDEX_NAME, node2.getType().getIndiceName(), node2.getIdString())
				.setSource(node2.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		List<ESNode> actual = entityDao.findAll(ESNode.class, 2l, 1l);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void findAllWays() {
		// Setup
		ESWay way1 = ESWay.Builder.create().id(1).addLocation(1.0d, 2.0d).build();
		ESWay way2 = ESWay.Builder.create().id(2).addLocation(3.0d, 4.0d).build();
		List<ESWay> expected = Arrays.asList(new ESWay[] { way1, way2 });

		client().prepareIndex(INDEX_NAME, way1.getType().getIndiceName(), way1.getIdString())
				.setSource(way1.toJson())
				.execute().actionGet();
		client().prepareIndex(INDEX_NAME, way2.getType().getIndiceName(), way2.getIdString())
				.setSource(way2.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		List<ESWay> actual = entityDao.findAll(ESWay.class, 1l, 2l);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void findAllWays_withSubset() {
		// Setup
		ESWay way1 = ESWay.Builder.create().id(1).addLocation(1.0d, 2.0d).build();
		ESWay way2 = ESWay.Builder.create().id(2).addLocation(3.0d, 4.0d).build();
		List<ESWay> expected = Arrays.asList(new ESWay[] { way2 });

		client().prepareIndex(INDEX_NAME, way1.getType().getIndiceName(), way1.getIdString())
				.setSource(way1.toJson())
				.execute().actionGet();
		client().prepareIndex(INDEX_NAME, way2.getType().getIndiceName(), way2.getIdString())
				.setSource(way2.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		List<ESWay> actual = entityDao.findAll(ESWay.class, 2l);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void findAllWays_keepOrder() {
		// Setup
		ESWay way1 = ESWay.Builder.create().id(1).addLocation(1.0d, 2.0d).build();
		ESWay way2 = ESWay.Builder.create().id(2).addLocation(3.0d, 4.0d).build();
		List<ESWay> expected = Arrays.asList(new ESWay[] { way2, way1 });

		client().prepareIndex(INDEX_NAME, way1.getType().getIndiceName(), way1.getIdString())
				.setSource(way1.toJson())
				.execute().actionGet();
		client().prepareIndex(INDEX_NAME, way2.getType().getIndiceName(), way2.getIdString())
				.setSource(way2.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		List<ESWay> actual = entityDao.findAll(ESWay.class, 2l, 1l);

		// Assert
		Assert.assertEquals(expected, actual);
	}

	/* delete */

	@Test
	public void deleteNode() {
		// Setup
		ESNode node = OsmDataBuilder.buildSampleESNode();
		client().prepareIndex(INDEX_NAME, node.getType().getIndiceName(), node.getIdString())
				.setSource(node.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		boolean actual = entityDao.delete(ESNode.class, 1l);

		// Assert
		Assert.assertTrue(actual);
	}

	@Test
	public void deleteNode_thatDoesNotExists() {
		// Setup
		ESNode node = OsmDataBuilder.buildSampleESNode();
		client().prepareIndex(INDEX_NAME, node.getType().getIndiceName(), node.getIdString())
				.setSource(node.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		boolean actual = entityDao.delete(ESNode.class, 2l);

		// Assert
		Assert.assertFalse(actual);
	}

	@Test
	public void deleteWay() {
		// Setup
		ESWay way = OsmDataBuilder.buildSampleESWay();
		client().prepareIndex(INDEX_NAME, way.getType().getIndiceName(), way.getIdString())
				.setSource(way.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		boolean actual = entityDao.delete(ESWay.class, 1l);

		// Assert
		Assert.assertTrue(actual);
	}

	@Test
	public void deleteWay_thatDoesNotExists() {
		// Setup
		ESWay way = OsmDataBuilder.buildSampleESWay();
		client().prepareIndex(INDEX_NAME, way.getType().getIndiceName(), way.getIdString())
				.setSource(way.toJson())
				.execute().actionGet();
		refresh(INDEX_NAME);

		// Action
		boolean actual = entityDao.delete(ESWay.class, 2l);

		// Assert
		Assert.assertFalse(actual);
	}

}
