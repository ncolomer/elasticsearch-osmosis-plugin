package org.openstreetmap.osmosis.plugin.elasticsearch.model;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.LocationArrayBuilder;

public class ESWay extends ESEntity {

	private final double[][] locations;

	private ESWay(Way way, LocationArrayBuilder locationArrayBuilder) {
		super(way);
		double[][] locations = locationArrayBuilder.toArray();
		if (locations.length != way.getWayNodes().size()) throw new IllegalArgumentException(String.format(
				"Incorrect size! WayNodes: %d, Locations: %d", way.getWayNodes().size(), locations.length));
		this.locations = locations;
	}

	private ESWay(Builder builder) {
		super(builder.id, builder.tags);
		this.locations = builder.locationArrayBuilder.toArray();
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
		try {
			boolean isClosed = isClosed();
			XContentBuilder builder = jsonBuilder();
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
		}
	}

	public static class Builder {

		private long id;
		private LocationArrayBuilder locationArrayBuilder = new LocationArrayBuilder();
		private Map<String, String> tags = new HashMap<String, String>();

		private Builder() {}

		public static Builder create() {
			return new Builder();
		}

		@SuppressWarnings("unchecked")
		public static ESWay buildFromGetReponse(GetResponse response) {
			if (!response.getType().equals(EntityDao.WAY)) throw new IllegalArgumentException("Provided GetResponse is not a Way");
			Builder builder = new Builder();
			builder.id = Long.valueOf(response.getId());
			builder.tags = (Map<String, String>) response.field("tags").getValue();
			List<List<Double>> locations = (List<List<Double>>) response.field("shape.coordinates").getValue();
			for (List<Double> location : locations) {
				builder.addLocation(location.get(1), location.get(0));
			}
			return builder.build();
		}

		public static ESWay buildFromWayEntity(Way way, LocationArrayBuilder locationArrayBuilder) {
			return new ESWay(way, locationArrayBuilder);
		}

		public Builder id(long id) {
			this.id = id;
			return this;
		}

		public Builder addLocation(double latitude, double longitude) {
			locationArrayBuilder.addLocation(latitude, longitude);
			return this;
		}

		public Builder tags(Map<String, String> tags) {
			this.tags = tags;
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
