package org.openstreetmap.osmosis.plugin.elasticsearch.model.entity;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESShape;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESShape.ESShapeBuilder;

public class ESWay extends ESEntity {

	private final double[][] locations;

	private ESWay(Way way, ESShape shape) {
		super(way);
		double[][] locations = shape.getGeoJsonArray();
		if (locations.length != way.getWayNodes().size()) throw new IllegalArgumentException(String.format(
				"Incorrect size! WayNodes: %d, Locations: %d", way.getWayNodes().size(), locations.length));
		this.locations = locations;
	}

	private ESWay(Builder builder) {
		super(builder.id, builder.tags);
		ESShape shape = builder.shapeBuilder.build();
		this.locations = shape.getGeoJsonArray();
	}

	@Override
	public ESEntityType getType() {
		return ESEntityType.WAY;
	}

	public double[][] getLocations() {
		return locations;
	}

	public boolean isClosed() {
		double[] first = locations[0];
		double[] last = locations[locations.length - 1];
		return Arrays.equals(first, last);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Arrays.hashCode(locations);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (getClass() != obj.getClass()) return false;
		ESWay other = (ESWay) obj;
		if (!Arrays.deepEquals(locations, other.locations)) return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Way [id=");
		builder.append(getId());
		builder.append(", isClosed=");
		builder.append(isClosed());
		builder.append(", tags=");
		builder.append(getTags());
		builder.append(", locations=");
		builder.append(Arrays.deepToString(locations));
		builder.append("]");
		return builder.toString();
	}

	@Override
	public String toJson() {
		XContentBuilder builder = null;
		try {
			boolean isClosed = isClosed();
			builder = jsonBuilder();
			builder.startObject();
			builder.startObject("shape");
			builder.field("type", isClosed ? "polygon" : "linestring");
			builder.startArray("coordinates");
			if (isClosed) builder.startArray();
			for (double[] location : locations) {
				builder.startArray().value(location[0]).value(location[1]).endArray();
			}
			if (isClosed) builder.endArray();
			builder.endArray();
			builder.endObject();
			builder.field("tags", getTags());
			builder.endObject();
			return builder.string();
		} catch (IOException e) {
			throw new RuntimeException("Unable to serialize Way to Json", e);
		} finally {
			if (builder != null) builder.close();
		}
	}

	public static class Builder {

		private long id;
		private ESShapeBuilder shapeBuilder = new ESShapeBuilder();
		private Map<String, String> tags = new HashMap<String, String>();

		private Builder() {}

		public static Builder create() {
			return new Builder();
		}

		@SuppressWarnings("unchecked")
		public static ESWay buildFromGetReponse(GetResponse response) {
			if (!response.getType().equals(ESEntityType.WAY.getIndiceName())) throw new IllegalArgumentException("Provided GetResponse is not a Way");
			Builder builder = new Builder();
			builder.id = Long.valueOf(response.getId());
			builder.tags = (Map<String, String>) response.getField("tags").getValue();
			Map<String, Object> shape = (Map<String, Object>) response.getField("shape").getValue();
			if (shape.get("type").equals("linestring")) {
				List<List<Double>> locations = (List<List<Double>>) shape.get("coordinates");
				for (List<Double> location : locations) {
					builder.addLocation(location.get(1), location.get(0));
				}
			} else {
				List<List<List<Double>>> locations = (List<List<List<Double>>>) shape.get("coordinates");
				for (List<Double> location : locations.get(0)) {
					builder.addLocation(location.get(1), location.get(0));
				}
			}

			return builder.build();
		}

		public static ESWay buildFromEntity(Way way, ESShape locationArrayBuilder) {
			return new ESWay(way, locationArrayBuilder);
		}

		public Builder id(long id) {
			this.id = id;
			return this;
		}

		public Builder addLocation(double latitude, double longitude) {
			shapeBuilder.addLocation(latitude, longitude);
			return this;
		}

		public Builder addTag(String key, String value) {
			this.tags.put(key, value);
			return this;
		}

		public ESWay build() {
			return new ESWay(this);
		}

	}

}
