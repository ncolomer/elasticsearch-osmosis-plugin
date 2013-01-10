package org.openstreetmap.osmosis.plugin.elasticsearch.integration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.elasticsearch.action.get.GetResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.OsmUser;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.index.osm.OsmIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;

public class EntityDaoIntTest extends AbstractElasticsearchInMemoryTest {

	private EntityDao entityDao;

	@Before
	public void setUp() throws IOException {
		IndexAdminService indexAdminService = new IndexAdminService(this.client());
		indexAdminService.createIndex("osm", new OsmIndexBuilder().getIndexMapping());
		entityDao = new EntityDao("osm", indexAdminService);
	}

	@After
	public void tearDown() {
		delete();
		refresh();
	}

	@Test
	public void saveNode() {
		// Setup
		List<Tag> tags = Arrays.asList(new Tag[] { new Tag("highway", "traffic_signals") });
		CommonEntityData entityData = new CommonEntityData(1l, 0, new Date(), new OsmUser(1, "nco"), 1l, tags);
		Node expectedNode = new Node(entityData, 1.0d, 2.0d);
		// Action
		entityDao.save((Entity) expectedNode);
		refresh("osm");
		// Assert
		GetResponse response = client().prepareGet("osm", "node", "1").execute().actionGet();
		Assert.assertTrue(response.exists());
		String expected = "{\"location\":[2.0,1.0],\"tags\":{\"highway\":\"traffic_signals\"}}";
		String actual = response.getSourceAsString();
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void saveNodeThenFind() {
		// Setup
		List<Tag> tags = Arrays.asList(new Tag[] { new Tag("highway", "traffic_signals") });
		CommonEntityData entityData = new CommonEntityData(1l, 0, new Date(), new OsmUser(1, "nco"), 1l, tags);
		Node expectedNode = new Node(entityData, 1.0d, 2.0d);
		entityDao.save((Entity) expectedNode);
		refresh("osm");
		// Action
		Node actual = entityDao.find(1l, Node.class);
		// Assert
		Assert.assertEquals(1l, actual.getId());
		Assert.assertEquals(1.0d, actual.getLatitude());
		Assert.assertEquals(2.0d, actual.getLongitude());
		Tag tag = actual.getTags().iterator().next();
		Assert.assertEquals("highway", tag.getKey());
		Assert.assertEquals("traffic_signals", tag.getValue());
	}

	@Test
	public void saveWay() {
		// Setup
		List<Tag> tags = Arrays.asList(new Tag[] { new Tag("highway", "residential") });
		CommonEntityData entityData = new CommonEntityData(1l, 0, new Date(), new OsmUser(1, "nco"), 1l, tags);
		Way way = new Way(entityData, Arrays.asList(new WayNode[] { new WayNode(1l) }));
		// Action
		entityDao.save((Entity) way);
		refresh("osm");
		// Assert
		GetResponse response = client().prepareGet("osm", "way", "1").execute().actionGet();
		Assert.assertTrue(response.exists());
		String expected = "{\"tags\":{\"highway\":\"residential\"},\"nodes\":[1]}";
		String actual = response.getSourceAsString();
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void saveWayThenFind() {
		// Setup
		List<Tag> tags = Arrays.asList(new Tag[] { new Tag("highway", "residential") });
		CommonEntityData entityData = new CommonEntityData(1l, 0, new Date(), new OsmUser(1, "nco"), 1l, tags);
		Way way = new Way(entityData, Arrays.asList(new WayNode[] { new WayNode(1l) }));
		entityDao.save((Entity) way);
		refresh("osm");
		// Action
		Way actual = entityDao.find(1l, Way.class);
		// Assert
		Assert.assertEquals(1l, actual.getId());
		Tag tag = actual.getTags().iterator().next();
		Assert.assertEquals("highway", tag.getKey());
		Assert.assertEquals("residential", tag.getValue());
		WayNode wayNode = actual.getWayNodes().get(0);
		Assert.assertEquals(1l, wayNode.getNodeId());
	}

}