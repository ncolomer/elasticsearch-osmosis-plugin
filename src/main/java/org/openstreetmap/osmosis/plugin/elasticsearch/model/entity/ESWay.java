package org.openstreetmap.osmosis.plugin.elasticsearch.model.entity;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESLocation;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESShape;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESShape.ESShapeBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESShapeType;

public class ESWay extends ESEntity {

	private final ESShape shape;

	private ESWay(Way way, ESShape shape) {
		super(way);
		double[][] locations = shape.getGeoJsonArray();
		if (locations.length != way.getWayNodes().size()) throw new IllegalArgumentException(String.format(
				"Incorrect size! WayNodes: %d, Shape: %d", way.getWayNodes().size(), locations.length));
		this.shape = shape;
	}

	private ESWay(Builder builder) {
		super(builder.id, builder.tags);
		this.shape = builder.shape;
	}

	@Override
	public ESEntityType getEntityType() {
		return ESEntityType.WAY;
	}

	@Override
	public ESShapeType getShapeType() {
		return shape.getShapeType();
	}

	@Override
	public ESLocation getCentroid() {
		return shape.getCentroid();
	}

	@Override
	public double getArea() {
		return shape.getAreaKm2();
	}

	@Override
	public double getLenght() {
		return shape.getLengthKm();
	}

	@Override
	public String toJson() {
		XContentBuilder builder = null;
		try {
			builder = jsonBuilder();
			builder.startObject();
			ESLocation centroid = shape.getCentroid();
			builder.field("centroid", new double[] { centroid.getLongitude(), centroid.getLatitude() });
			builder.field("lengthKm", shape.getLengthKm());
			builder.field("areaKm2", shape.getAreaKm2());
			builder.startObject("shape");
			builder.field("type", shape.isClosed() ? "polygon" : "linestring");
			builder.startArray("coordinates");
			if (shape.isClosed()) builder.startArray();
			for (double[] location : shape.getGeoJsonArray()) {
				builder.startArray().value(location[0]).value(location[1]).endArray();
			}
			if (shape.isClosed()) builder.endArray();
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((shape == null) ? 0 : shape.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (getClass() != obj.getClass()) return false;
		ESWay other = (ESWay) obj;
		if (shape == null) {
			if (other.shape != null) return false;
		} else if (!shape.equals(other.shape)) return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ESWay [id=");
		builder.append(getId());
		builder.append(", shape=");
		builder.append(shape);
		builder.append(", tags=");
		builder.append(getTags());
		builder.append("]");
		return builder.toString();
	}

	public static class Builder {

		private ESShapeBuilder shapeBuilder = new ESShapeBuilder();

		private long id;
		private ESShape shape;
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
			String type = (String) shape.get("type");
			if ("linestring".equals(type)) {
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

			List<Double> centroid = (List<Double>) response.getField("centroid").getValue();
			builder.shapeBuilder.setCentroid(new ESLocation(centroid.get(1), centroid.get(0)));
			Double length = (Double) response.getField("lengthKm").getValue();
			builder.shapeBuilder.setLength(length);
			Double area = (Double) response.getField("areaKm2").getValue();
			builder.shapeBuilder.setArea(area);

			builder.shape = builder.shapeBuilder.buildFast();
			return new ESWay(builder);
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
			this.shape = shapeBuilder.build();
			return new ESWay(this);
		}

	}

}
