package org.openstreetmap.osmosis.plugin.elasticsearch.model;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.junit.Test;
import org.mockito.Mockito;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.LocationArrayBuilder;

public class ESWayUTest {

	@Test
	public void buildFromWayEntity() {
		// Setup
		Way way = mock(Way.class);
		when(way.getId()).thenReturn(1l);
		List<Tag> tags = new ArrayList<Tag>();
		tags.add(new Tag("highway", "primary"));
		when(way.getTags()).thenReturn(tags);
		List<WayNode> wayNodes = new ArrayList<WayNode>();
		wayNodes.add(new WayNode(1l));
		wayNodes.add(new WayNode(2l));
		when(way.getWayNodes()).thenReturn(wayNodes);

		LocationArrayBuilder builder = new LocationArrayBuilder();
		builder.addLocation(11.0, 12.0).addLocation(21.0, 22.0);

		ESWay expected = ESWay.Builder.create().id(1l)
				.addLocation(11.0, 12.0).addLocation(21.0, 22.0)
				.addTag("highway", "primary").build();

		// Action
		ESWay actual = ESWay.Builder.buildFromEntity(way, builder);

		// Assert
		assertEquals(expected, actual);
	}

	@Test(expected = IllegalArgumentException.class)
	public void buildFromWayEntity_withIncorrectWayNodeSize() {
		// Setup
		Way way = mock(Way.class);
		when(way.getId()).thenReturn(1l);
		List<Tag> tags = new ArrayList<Tag>();
		tags.add(new Tag("highway", "primary"));
		when(way.getTags()).thenReturn(tags);
		List<WayNode> wayNodes = new ArrayList<WayNode>();
		wayNodes.add(new WayNode(1l));
		wayNodes.add(new WayNode(2l));
		when(way.getWayNodes()).thenReturn(wayNodes);

		LocationArrayBuilder builder = new LocationArrayBuilder();
		builder.addLocation(11.0, 12.0);

		// Action
		ESWay.Builder.buildFromEntity(way, builder);
	}

	@Test
	public void buildFromGetReponse() {
		// Setup
		GetResponse response = mock(GetResponse.class, Mockito.RETURNS_DEEP_STUBS);
		when(response.getType()).thenReturn(ESEntityType.WAY.getIndiceName());
		when(response.getId()).thenReturn("1");
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("highway", "primary");
		when(response.getField("tags").getValue()).thenReturn(tags);
		List<List<List<Double>>> locations = new ArrayList<List<List<Double>>>();
		ArrayList<List<Double>> subLocations = new ArrayList<List<Double>>();
		subLocations.add(Arrays.asList(new Double[] { 12.0, 11.0 }));
		subLocations.add(Arrays.asList(new Double[] { 22.0, 21.0 }));
		locations.add(subLocations);
		@SuppressWarnings("unchecked")
		Map<String, Object> shape = mock(Map.class);
		when(shape.get("coordinates")).thenReturn(locations);
		when(response.getField("shape").getValue()).thenReturn(shape);

		ESWay expected = ESWay.Builder.create().id(1l)
				.addLocation(11.0, 12.0).addLocation(21.0, 22.0)
				.addTag("highway", "primary").build();

		// Action
		ESWay actual = ESWay.Builder.buildFromGetReponse(response);

		// Assert
		assertEquals(expected, actual);
	}

	@Test(expected = IllegalArgumentException.class)
	public void buildFromGetReponse_withInvalidGetResponse() {
		// Setup
		GetResponse response = mock(GetResponse.class, Mockito.RETURNS_DEEP_STUBS);
		when(response.getType()).thenReturn(ESEntityType.NODE.getIndiceName());

		// Action
		ESNode.Builder.buildFromGetReponse(response);
	}

	@Test
	public void getType() {
		// Setup
		ESWay way = ESWay.Builder.create().id(1l)
				.addLocation(11.0, 12.0).addLocation(21.0, 22.0)
				.addTag("highway", "primary").build();

		// Action
		ESEntityType actual = way.getType();

		// Assert
		assertEquals(ESEntityType.WAY, actual);
	}

	@Test
	public void isClosed() {
		// Setup
		ESWay way1 = ESWay.Builder.create().id(1l)
				.addLocation(11.0, 12.0).addLocation(21.0, 22.0)
				.addTag("highway", "primary").build();

		ESWay way2 = ESWay.Builder.create().id(2l)
				.addLocation(11.0, 12.0).addLocation(21.0, 22.0).addLocation(11.0, 12.0)
				.addTag("highway", "primary").build();

		// Assert
		assertEquals(false, way1.isClosed());
		assertEquals(true, way2.isClosed());
	}

	@Test
	public void toJson_withLinestring() {
		// Setup
		ESWay way = ESWay.Builder.create().id(1l)
				.addLocation(11.0, 12.0).addLocation(21.0, 22.0)
				.addTag("highway", "primary").build();
		String expected = "{\"shape\":{\"type\":\"linestring\",\"coordinates\":" +
				"[[12.0,11.0],[22.0,21.0]]},\"tags\":{\"highway\":\"primary\"}}";

		// Action
		String actual = way.toJson();

		// Assert
		assertEquals(expected, actual);
	}

	@Test
	public void toJson_withPolygon() {
		// Setup
		ESWay way = ESWay.Builder.create().id(1l)
				.addLocation(11.0, 12.0).addLocation(21.0, 22.0).addLocation(11.0, 12.0)
				.addTag("highway", "primary").build();
		String expected = "{\"shape\":{\"type\":\"polygon\",\"coordinates\":" +
				"[[[12.0,11.0],[22.0,21.0],[12.0,11.0]]]},\"tags\":{\"highway\":\"primary\"}}";

		// Action
		String actual = way.toJson();

		// Assert
		assertEquals(expected, actual);
	}

}
