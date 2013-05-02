package org.openstreetmap.osmosis.plugin.elasticsearch.model.entity;

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
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESShape.ESShapeBuilder;

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

		ESShapeBuilder builder = new ESShapeBuilder();
		builder.addLocation(1.0, 2.0).addLocation(2.0, 3.0);

		ESWay expected = ESWay.Builder.create().id(1l)
				.addLocation(1.0, 2.0).addLocation(2.0, 3.0)
				.addTag("highway", "primary").build();

		// Action
		ESWay actual = ESWay.Builder.buildFromEntity(way, builder.build());

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

		ESShapeBuilder builder = new ESShapeBuilder();
		builder.addLocation(1.0, 2.0);

		// Action
		ESWay.Builder.buildFromEntity(way, builder.build());
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
		subLocations.add(Arrays.asList(new Double[] { 2.0, 1.0 }));
		subLocations.add(Arrays.asList(new Double[] { 3.0, 2.0 }));
		locations.add(subLocations);
		@SuppressWarnings("unchecked")
		Map<String, Object> shape = mock(Map.class);
		when(shape.get("type")).thenReturn("polygon");
		when(shape.get("coordinates")).thenReturn(locations);
		when(response.getField("shape").getValue()).thenReturn(shape);
		when(response.getField("centroid").getValue()).thenReturn(Arrays.asList(new Double[] { 2.5, 1.5 }));
		when(response.getField("lengthKm").getValue()).thenReturn(157.25358982950198d);
		when(response.getField("areaKm2").getValue()).thenReturn(0d);

		ESWay expected = ESWay.Builder.create().id(1l)
				.addLocation(1.0, 2.0).addLocation(2.0, 3.0)
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
				.addLocation(1.0, 2.0).addLocation(2.0, 3.0)
				.addTag("highway", "primary").build();

		// Action
		ESEntityType actual = way.getEntityType();

		// Assert
		assertEquals(ESEntityType.WAY, actual);
	}

	@Test
	public void toJson_withLinestring() {
		// Setup
		ESWay way = ESWay.Builder.create().id(1l)
				.addLocation(1.0, 2.0).addLocation(2.0, 3.0)
				.addTag("highway", "primary").build();
		String expected = "{\"centroid\":[2.5,1.5],\"lengthKm\":157.25358982950198," +
				"\"areaKm2\":0.0,\"shape\":{\"type\":\"linestring\",\"coordinates\":" +
				"[[2.0,1.0],[3.0,2.0]]},\"tags\":{\"highway\":\"primary\"}}";

		// Action
		String actual = way.toJson();

		// Assert
		assertEquals(expected, actual);
	}

	@Test
	public void toJson_withPolygon() {
		// Setup
		ESWay way = ESWay.Builder.create().id(1l)
				.addLocation(1.0, 2.0).addLocation(2.0, 3.0)
				.addLocation(3.0, 2.0).addLocation(1.0, 2.0)
				.addTag("highway", "primary").build();
		String expected = "{\"centroid\":[2.3333333333333335,2.0],\"lengthKm\":536.8973391277414," +
				"\"areaKm2\":12364.345757132623,\"shape\":{\"type\":\"polygon\",\"coordinates\":" +
				"[[[2.0,1.0],[3.0,2.0],[2.0,3.0],[2.0,1.0]]]},\"tags\":{\"highway\":\"primary\"}}";

		// Action
		String actual = way.toJson();

		// Assert
		assertEquals(expected, actual);
	}

}
