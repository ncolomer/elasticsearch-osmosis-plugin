package org.openstreetmap.osmosis.plugin.elasticsearch.model.shape;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.common.geo.GeoShapeConstants;

import com.spatial4j.core.distance.DistanceUtils;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class ESShape {

	private final ESShapeType esShapeType;
	private final ESLocation centroid;
	private final double length;
	private final double area;
	private final double[][] geoJsonArray;

	private ESShape(ESShapeBuilder builder) {
		this.esShapeType = builder.esShapeType;
		this.area = builder.area;
		this.length = builder.length;
		this.centroid = builder.centroid;
		this.geoJsonArray = builder.geoJsonArray;
	}

	public ESShapeType getShapeType() {
		return esShapeType;
	}

	public boolean isClosed() {
		switch (esShapeType) {
		case POINT:
			return true;
		case LINESTRING:
			return false;
		case POLYGON:
			return true;
		default:
			return false;
		}
	}

	public ESLocation getCentroid() {
		return centroid;
	}

	public double getLengthKm() {
		return length;
	}

	public double getAreaKm2() {
		return area;
	}

	public double[][] getGeoJsonArray() {
		return geoJsonArray;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(area);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((centroid == null) ? 0 : centroid.hashCode());
		result = prime * result + ((esShapeType == null) ? 0 : esShapeType.hashCode());
		result = prime * result + Arrays.hashCode(geoJsonArray);
		temp = Double.doubleToLongBits(length);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		ESShape other = (ESShape) obj;
		if (Double.doubleToLongBits(area) != Double.doubleToLongBits(other.area)) return false;
		if (centroid == null) {
			if (other.centroid != null) return false;
		} else if (!centroid.equals(other.centroid)) return false;
		if (esShapeType != other.esShapeType) return false;
		if (!Arrays.deepEquals(geoJsonArray, other.geoJsonArray)) return false;
		if (Double.doubleToLongBits(length) != Double.doubleToLongBits(other.length)) return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ESShape [esShapeType=");
		builder.append(esShapeType);
		builder.append(", isClosed=");
		builder.append(isClosed());
		builder.append(", centroid=");
		builder.append(centroid);
		builder.append(", lenght=");
		builder.append(length);
		builder.append(", area=");
		builder.append(area);
		builder.append(", geoJsonArray=");
		builder.append(Arrays.deepToString(geoJsonArray));
		builder.append("]");
		return builder.toString();
	}

	public static class ESShapeBuilder {

		private ESShapeType esShapeType;
		private double area;
		private double length;
		private ESLocation centroid;
		private double[][] geoJsonArray;

		/*
		 * REGULAR BUILDER
		 */

		public void setArea(double area) {
			this.area = area;
		}

		public void setLength(double length) {
			this.length = length;
		}

		public void setCentroid(ESLocation centroid) {
			this.centroid = centroid;
		}

		public ESShape buildFast() {
			this.esShapeType = getShapeType();
			this.geoJsonArray = toGeoJsonArray();
			return new ESShape(this);
		}

		/*
		 * SPECIALIZED BUILDER (FROM LOCATIONS)
		 */

		private final List<ESLocation> locations;

		public ESShapeBuilder() {
			this.locations = new ArrayList<ESLocation>();
		}

		public ESShapeBuilder(int size) {
			this.locations = new ArrayList<ESLocation>(size);
		}

		public ESShapeBuilder addLocation(double latitude, double longitude) {
			locations.add(new ESLocation(latitude, longitude));
			return this;
		}

		public ESShape build() {
			this.esShapeType = getShapeType();
			Geometry geometry = buildGeometry();
			this.area = degree2ToKm2(geometry.getArea());
			this.length = degreeToKm(geometry.getLength());
			Point centroid = geometry.getCentroid();
			this.centroid = new ESLocation(centroid.getY(), centroid.getX());
			this.geoJsonArray = toGeoJsonArray();
			return new ESShape(this);
		}

		private boolean isClosed() {
			ESLocation first = locations.get(0);
			ESLocation last = locations.get(locations.size() - 1);
			return first.equals(last);
		}

		private ESShapeType getShapeType() {
			if (locations.isEmpty()) {
				throw new IllegalStateException("This builder contains no location");
			} else if (locations.size() == 1) {
				return ESShapeType.POINT;
			} else if (!isClosed()) {
				return ESShapeType.LINESTRING;
			} else {
				return ESShapeType.POLYGON;
			}
		}

		private Geometry buildGeometry() {
			Coordinate[] coordinates = new Coordinate[locations.size()];
			for (int i = 0; i < locations.size(); i++) {
				coordinates[i] = new Coordinate(
						locations.get(i).getLongitude(),
						locations.get(i).getLatitude());
			}
			GeometryFactory factory = GeoShapeConstants.SPATIAL_CONTEXT.getGeometryFactory();
			CoordinateSequence sequence = factory.getCoordinateSequenceFactory().create(coordinates);
			switch (getShapeType()) {
			case POINT:
				return new Point(sequence, factory);
			case LINESTRING:
				return new LineString(sequence, factory);
			case POLYGON:
				LinearRing shell = new LinearRing(sequence, factory);
				return new Polygon(shell, null, factory);
			default:
				throw new IllegalStateException("Unrecognized geometry");
			}
		}

		private double[][] toGeoJsonArray() {
			double[][] array = new double[locations.size()][2];
			for (int i = 0; i < locations.size(); i++) {
				array[i] = locations.get(i).toGeoJsonArray();
			}
			return array;
		}

		private static double degree2ToKm2(double degree2Area) {
			double degreeArea = Math.sqrt(degree2Area);
			double kmArea = DistanceUtils.degrees2Dist(degreeArea, DistanceUtils.EARTH_MEAN_RADIUS_KM);
			double km2Area = Math.pow(kmArea, 2);
			return km2Area;
		}

		private static double degreeToKm(double degreeLength) {
			return DistanceUtils.degrees2Dist(degreeLength, DistanceUtils.EARTH_MEAN_RADIUS_KM);
		}

	}

}