package org.openstreetmap.osmosis.plugin.elasticsearch.model.entity;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESLocation;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESShapeType;

public class ESNode extends ESEntity {

	private final double latitude;
	private final double longitude;

	private ESNode(Node node) {
		super(node);
		this.latitude = node.getLatitude();
		this.longitude = node.getLongitude();
	}

	private ESNode(Builder builder) {
		super(builder.id, builder.tags);
		this.latitude = builder.latitude;
		this.longitude = builder.longitude;
	}

	@Override
	public ESEntityType getEntityType() {
		return ESEntityType.NODE;
	}

	@Override
	public ESShapeType getShapeType() {
		return ESShapeType.POINT;
	}

	@Override
	public ESLocation getCentroid() {
		return new ESLocation(latitude, longitude);
	}

	@Override
	public double getLenght() {
		return 0;
	}

	@Override
	public double getArea() {
		return 0;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	@Override
	public String toJson() {
		XContentBuilder builder = null;
		try {
			builder = jsonBuilder();
			builder.startObject();
			builder.field("centroid", new double[] { longitude, latitude });
			builder.startObject("shape")
					.field("type", "point")
					.field("coordinates", new double[] { longitude, latitude })
					.endObject();
			builder.field("tags", getTags());
			builder.endObject();
			return builder.string();
		} catch (IOException e) {
			throw new RuntimeException("Unable to serialize Node to Json", e);
		} finally {
			if (builder != null) builder.close();
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		long temp;
		temp = Double.doubleToLongBits(latitude);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(longitude);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (getClass() != obj.getClass()) return false;
		ESNode other = (ESNode) obj;
		if (Double.doubleToLongBits(latitude) != Double.doubleToLongBits(other.latitude)) return false;
		if (Double.doubleToLongBits(longitude) != Double.doubleToLongBits(other.longitude)) return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ESNode [id=");
		builder.append(getId());
		builder.append(", lat=");
		builder.append(latitude);
		builder.append(", lon=");
		builder.append(longitude);
		builder.append(", tags=");
		builder.append(getTags());
		builder.append("]");
		return builder.toString();
	}

	public static class Builder {

		private long id;
		private double latitude;
		private double longitude;
		private Map<String, String> tags = new HashMap<String, String>();

		private Builder() {}

		public static Builder create() {
			return new Builder();
		}

		@SuppressWarnings("unchecked")
		public static ESNode buildFromGetReponse(GetResponse response) {
			if (!response.getType().equals(ESEntityType.NODE.getIndiceName())) throw new IllegalArgumentException("Provided GetResponse is not a Node");
			Builder builder = new Builder();
			builder.id = Long.valueOf(response.getId());
			builder.tags = (Map<String, String>) response.getField("tags").getValue();
			Map<String, Object> shape = (Map<String, Object>) response.getField("shape").getValue();
			List<Double> location = (List<Double>) shape.get("coordinates");
			builder.latitude = location.get(1);
			builder.longitude = location.get(0);
			return builder.build();
		}

		public static ESNode buildFromEntity(Node node) {
			return new ESNode(node);
		}

		public Builder id(long id) {
			this.id = id;
			return this;
		}

		public Builder location(double latitude, double longitude) {
			this.latitude = latitude;
			this.longitude = longitude;
			return this;
		}

		public Builder addTag(String key, String value) {
			this.tags.put(key, value);
			return this;
		}

		public ESNode build() {
			return new ESNode(this);
		}

	}

}
