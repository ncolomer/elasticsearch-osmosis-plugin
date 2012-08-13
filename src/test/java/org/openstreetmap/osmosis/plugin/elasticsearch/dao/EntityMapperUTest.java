package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

public class EntityMapperUTest {

	private EntityMapper entityMapper;

	@Before
	public void setUp() {
		entityMapper = new EntityMapper();
	}

	@Test
	public void marshallNode() throws IOException {
		// Setup
		Node node = mock(Node.class);
		when(node.getType()).thenReturn(EntityType.Node);
		when(node.getId()).thenReturn(1l);
		when(node.getLatitude()).thenReturn(1.0d);
		when(node.getLongitude()).thenReturn(2.0d);
		List<Tag> tags = Arrays.asList(new Tag[] { new Tag("highway", "traffic_signals") });
		when(node.getTags()).thenReturn(tags);
		// Action
		String actual = entityMapper.marshallNode(node).string();
		// Assert
		String expected = "{\"location\":[2.0,1.0],\"tags\":{\"highway\":\"traffic_signals\"}}";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void unmarshallNode() throws IOException {
		// Setup
		SearchHit hit = mock(SearchHit.class);
		when(hit.getType()).thenReturn("node");
		when(hit.getId()).thenReturn("1");

		SearchHitField tagsHitField = mock(SearchHitField.class);
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("highway", "traffic_signals");
		when(tagsHitField.getValue()).thenReturn(tags);
		when(hit.field("tags")).thenReturn(tagsHitField);

		SearchHitField locationHitField = mock(SearchHitField.class);
		when(locationHitField.getValue()).thenReturn(Arrays.asList(new Double[] { 2.0d, 1.0d }));
		when(hit.field("location")).thenReturn(locationHitField);
		// Action
		Node node = entityMapper.unmarshallNode(hit);
		// Assert
		Assert.assertEquals(1l, node.getId());
		Tag actualTag = node.getTags().iterator().next();
		Assert.assertEquals("highway", actualTag.getKey());
		Assert.assertEquals("traffic_signals", actualTag.getValue());
		Assert.assertEquals(1.0, node.getLatitude());
		Assert.assertEquals(2.0, node.getLongitude());
	}

	@Test(expected = IllegalArgumentException.class)
	public void unmarshallNode_withWrongType() throws IOException {
		// Setup
		SearchHit hit = mock(SearchHit.class);
		when(hit.getType()).thenReturn("way");
		// Action
		entityMapper.unmarshallNode(hit);
	}

	@Test
	public void marshallWay() throws IOException {
		// Setup
		Way way = mock(Way.class);
		when(way.getType()).thenReturn(EntityType.Way);
		when(way.getId()).thenReturn(1l);
		List<WayNode> nodes = Arrays.asList(new WayNode[] { new WayNode(1l) });
		when(way.getWayNodes()).thenReturn(nodes);
		List<Tag> tags = Arrays.asList(new Tag[] { new Tag("highway", "residential") });
		when(way.getTags()).thenReturn(tags);
		// Action
		String actual = entityMapper.marshallWay(way).string();
		// Assert
		String expected = "{\"tags\":{\"highway\":\"residential\"},\"nodes\":[1]}";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void unmarshallWay() throws IOException {
		// Setup
		SearchHit hit = mock(SearchHit.class);
		when(hit.getType()).thenReturn("way");
		when(hit.getId()).thenReturn("1");

		SearchHitField tagsHitField = mock(SearchHitField.class);
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("highway", "residential");
		when(tagsHitField.getValue()).thenReturn(tags);
		when(hit.field("tags")).thenReturn(tagsHitField);

		SearchHitField nodesHitField = mock(SearchHitField.class);
		when(nodesHitField.getValues()).thenReturn(Arrays.asList(new Object[] { 1l, 2l }));
		when(hit.field("nodes")).thenReturn(nodesHitField);
		// Action
		Way way = entityMapper.unmarshallWay(hit);
		// Assert
		Assert.assertEquals(1l, way.getId());
		Tag actualTag = way.getTags().iterator().next();
		Assert.assertEquals("highway", actualTag.getKey());
		Assert.assertEquals("residential", actualTag.getValue());
		Assert.assertEquals(1l, way.getWayNodes().get(0).getNodeId());
		Assert.assertEquals(2l, way.getWayNodes().get(1).getNodeId());
	}

	@Test(expected = IllegalArgumentException.class)
	public void unmarshallWay_withWrongType() throws IOException {
		// Setup
		SearchHit hit = mock(SearchHit.class);
		when(hit.getType()).thenReturn("node");
		// Action
		entityMapper.unmarshallWay(hit);
	}

}
