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
import org.elasticsearch.search.SearchHit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.AssertUtils;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.OsmDataBuilder;

public class EntityMapperUTest {

	private EntityMapper entityMapper;

	@Before
	public void setUp() {
		entityMapper = new EntityMapper();
	}

	@Test
	public void marshallNode() throws IOException {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();

		// Action
		String actual = entityMapper.marshallNode(node).string();

		// Assert
		String expected = "{\"location\":[2.0,1.0],\"tags\":{\"highway\":\"traffic_signals\"}}";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void unmarshallNode_searchHit() throws IOException {
		// Setup
		Node expected = OsmDataBuilder.buildSampleNode();

		SearchHit hit = mock(SearchHit.class, Mockito.RETURNS_DEEP_STUBS);
		when(hit.getType()).thenReturn("node");
		when(hit.getId()).thenReturn("1");

		Map<String, String> tags = new HashMap<String, String>();
		tags.put("highway", "traffic_signals");
		when(hit.field("tags").getValue()).thenReturn(tags);

		List<Double> location = Arrays.asList(new Double[] { 2.0d, 1.0d });
		when(hit.field("location").getValue()).thenReturn(location);

		// Action
		Node actual = entityMapper.unmarshallNode(hit);

		// Assert
		AssertUtils.assertEquals(expected, actual);
	}

	@Test(expected = IllegalArgumentException.class)
	public void unmarshallNode_searchHit_withWrongType() throws IOException {
		// Setup
		SearchHit hit = mock(SearchHit.class);
		when(hit.getType()).thenReturn("way");

		// Action
		entityMapper.unmarshallNode(hit);
	}

	@Test
	public void unmarshallNode_getResponse() throws IOException {
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
		Node actual = entityMapper.unmarshallNode(get);

		// Assert
		AssertUtils.assertEquals(expected, actual);
	}

	@Test(expected = IllegalArgumentException.class)
	public void unmarshallNode_getResponse_withWrongType() throws IOException {
		// Setup
		GetResponse get = mock(GetResponse.class);
		when(get.getType()).thenReturn("way");

		// Action
		entityMapper.unmarshallNode(get);
	}

	@Test
	public void marshallWay() throws IOException {
		// Setup
		Way way = OsmDataBuilder.buildSampleWay();

		// Action
		String actual = entityMapper.marshallWay(way).string();

		// Assert
		String expected = "{\"tags\":{\"highway\":\"residential\"},\"nodes\":[1]}";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void unmarshallWay_searchHit() throws IOException {
		// Setup
		Way expected = OsmDataBuilder.buildSampleWay();

		SearchHit hit = mock(SearchHit.class, Mockito.RETURNS_DEEP_STUBS);
		when(hit.getType()).thenReturn("way");
		when(hit.getId()).thenReturn("1");

		Map<String, String> tags = new HashMap<String, String>();
		tags.put("highway", "residential");
		when(hit.field("tags").getValue()).thenReturn(tags);

		List<Object> nodes = Arrays.asList(new Object[] { 1l });
		when(hit.field("nodes").getValues()).thenReturn(nodes);

		// Action
		Way actual = entityMapper.unmarshallWay(hit);

		// Assert
		AssertUtils.assertEquals(expected, actual);
	}

	@Test(expected = IllegalArgumentException.class)
	public void unmarshallWay_searchHit_withWrongType() throws IOException {
		// Setup
		SearchHit hit = mock(SearchHit.class);
		when(hit.getType()).thenReturn("node");

		// Action
		entityMapper.unmarshallWay(hit);
	}

	@Test
	public void unmarshallWay_getResponse() throws IOException {
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
		Way actual = entityMapper.unmarshallWay(get);

		// Assert
		AssertUtils.assertEquals(expected, actual);
	}

	@Test(expected = IllegalArgumentException.class)
	public void unmarshallWay_getResponse_withWrongType() throws IOException {
		// Setup
		GetResponse hit = mock(GetResponse.class);
		when(hit.getType()).thenReturn("node");

		// Action
		entityMapper.unmarshallWay(hit);
	}

}
