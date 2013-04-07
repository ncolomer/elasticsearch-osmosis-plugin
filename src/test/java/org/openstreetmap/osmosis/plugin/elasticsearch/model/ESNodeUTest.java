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
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

public class ESNodeUTest {

	@Test
	public void buildFromNodeEntity() {
		// Setup
		Node node = mock(Node.class);
		when(node.getId()).thenReturn(1l);
		List<Tag> tags = new ArrayList<Tag>();
		tags.add(new Tag("highway", "primary"));
		when(node.getTags()).thenReturn(tags);
		when(node.getLatitude()).thenReturn(1.0);
		when(node.getLongitude()).thenReturn(2.0);

		ESNode expected = ESNode.Builder.create().id(1l).location(1.0, 2.0)
				.addTag("highway", "primary").build();

		// Action
		ESNode actual = ESNode.Builder.buildFromEntity(node);

		// Assert
		assertEquals(expected, actual);
	}

	@Test
	public void buildFromGetReponse() {
		// Setup
		GetResponse response = mock(GetResponse.class, Mockito.RETURNS_DEEP_STUBS);
		when(response.getType()).thenReturn(ESEntityType.NODE.getIndiceName());
		when(response.getId()).thenReturn("1");
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("highway", "primary");
		when(response.field("tags").getValue()).thenReturn(tags);
		List<Double> location = Arrays.asList(new Double[] { 2.0, 1.0 });
		@SuppressWarnings("unchecked")
		Map<String, Object> shape = mock(Map.class);
		when(shape.get("coordinates")).thenReturn(location);
		when(response.field("shape").getValue()).thenReturn(shape);

		ESNode expected = ESNode.Builder.create().id(1l).location(1.0, 2.0)
				.addTag("highway", "primary").build();

		// Action
		ESNode actual = ESNode.Builder.buildFromGetReponse(response);

		// Assert
		assertEquals(expected, actual);
	}

	@Test(expected = IllegalArgumentException.class)
	public void buildFromGetReponse_withInvalidGetResponse() {
		// Setup
		GetResponse response = mock(GetResponse.class, Mockito.RETURNS_DEEP_STUBS);
		when(response.getType()).thenReturn(ESEntityType.WAY.getIndiceName());

		// Action
		ESNode.Builder.buildFromGetReponse(response);
	}

	@Test
	public void getType() {
		// Setup
		ESNode node = ESNode.Builder.create().id(1l).location(1.0, 2.0)
				.addTag("highway", "primary").build();

		// Action
		ESEntityType actual = node.getType();

		// Assert
		assertEquals(ESEntityType.NODE, actual);
	}

	@Test
	public void toJson() {
		// Setup
		ESNode node = ESNode.Builder.create().id(1l).location(1.0, 2.0)
				.addTag("highway", "primary").build();
		String expected = "{\"shape\":{\"type\":\"point\",\"coordinates\":[2.0,1.0]}," +
				"\"tags\":{\"highway\":\"primary\"}}";

		// Action
		String actual = node.toJson();

		// Assert
		assertEquals(expected, actual);
	}

}
