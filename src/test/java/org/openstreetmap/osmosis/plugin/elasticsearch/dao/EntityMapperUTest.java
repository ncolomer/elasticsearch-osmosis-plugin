package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.action.get.GetResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.openstreetmap.osmosis.core.domain.v0_6.Bound;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.AssertUtils;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.OsmDataBuilder;

public class EntityMapperUTest {

	private EntityMapper entityMapper;

	@Before
	public void setUp() {
		entityMapper = new EntityMapper();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void marshall_withRelation() throws IOException {
		// Setup
		Relation relation = mock(Relation.class);
		when(relation.getType()).thenReturn(EntityType.Relation);

		// Action
		entityMapper.marshall(relation);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void marshall_withBound() throws IOException {
		// Setup
		Bound bound = mock(Bound.class);
		when(bound.getType()).thenReturn(EntityType.Relation);

		// Action
		entityMapper.marshall(bound);
	}

	@Test
	public void marshallNode() throws IOException {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();

		// Action
		String actual = entityMapper.marshall(node).string();

		// Assert
		String expected = "{\"location\":[2.0,1.0],\"tags\":{\"highway\":\"traffic_signals\"}}";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void unmarshallNode_withGetResponse() throws IOException {
		// Setup
		Node expected = OsmDataBuilder.buildSampleNode();

		GetResponse get = mock(GetResponse.class, Mockito.RETURNS_DEEP_STUBS);
		when(get.getType()).thenReturn("node");
		when(get.getId()).thenReturn("1");

		Map<String, String> tags = new HashMap<String, String>();
		tags.put("highway", "traffic_signals");
		when(get.field("tags").getValue()).thenReturn(tags);

		List<Double> location = Arrays.asList(new Double[] { 2.0d, 1.0d });
		when(get.field("location").getValue()).thenReturn(location);

		// Action
		Node actual = entityMapper.unmarshall(EntityType.Node, get);

		// Assert
		AssertUtils.assertEquals(expected, actual);
	}

	@Test(expected = IllegalArgumentException.class)
	public void unmarshallNode_withGetResponseAndWrongType() throws IOException {
		// Setup
		GetResponse get = mock(GetResponse.class);
		when(get.getType()).thenReturn("way");

		// Action
		entityMapper.unmarshall(EntityType.Node, get);
	}

	@Test
	public void marshallWay() throws IOException {
		// Setup
		Way way = OsmDataBuilder.buildSampleWay();

		// Action
		String actual = entityMapper.marshall(way).string();

		// Assert
		String expected = "{\"tags\":{\"highway\":\"residential\"},\"nodes\":[1]}";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void unmarshallWay_withGetResponse() throws IOException {
		// Setup
		Way expected = OsmDataBuilder.buildSampleWay();

		GetResponse get = mock(GetResponse.class, Mockito.RETURNS_DEEP_STUBS);
		when(get.getType()).thenReturn("way");
		when(get.getId()).thenReturn("1");

		Map<String, String> tags = new HashMap<String, String>();
		tags.put("highway", "residential");
		when(get.field("tags").getValue()).thenReturn(tags);

		List<Object> nodes = Arrays.asList(new Object[] { 1l });
		when(get.field("nodes").getValue()).thenReturn(nodes);

		// Action
		Way actual = entityMapper.unmarshall(EntityType.Way, get);

		// Assert
		AssertUtils.assertEquals(expected, actual);
	}

	@Test(expected = IllegalArgumentException.class)
	public void unmarshallWay_withGetResponseAndWrongType() throws IOException {
		// Setup
		GetResponse get = mock(GetResponse.class);
		when(get.getType()).thenReturn("node");

		// Action
		entityMapper.unmarshall(EntityType.Way, get);
	}

}
