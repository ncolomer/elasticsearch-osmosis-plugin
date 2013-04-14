package org.openstreetmap.osmosis.plugin.elasticsearch.model.shape;

import java.util.ArrayList;
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

	private ESShapeType esShapeType;
	private boolean isClosed;
	private double area;
	private double lenght;
	private ESLocation centroid;
	private double[][] geoJsonArray;

	private ESShape() {}

	public ESShapeType getType() {
		return esShapeType;
	}

	public boolean isClosed() {
		return isClosed;
	}

	public double getAreaKm2() {
		return area;
	}

	public double getLenghtKm() {
		return lenght;
	}

	public ESLocation getCentroid() {
		return centroid;
	}

	public double[][] getGeoJsonArray() {
		return geoJsonArray;
	}

	public static class ESShapeBuilder {

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
			ESShape shape = new ESShape();
			shape.isClosed = isClosed();
			shape.esShapeType = getShapeType();
			Geometry geometry = buildGeometry();
			shape.area = degree2ToKm2(geometry.getArea());
			shape.lenght = degreeToKm(geometry.getLength());
			Point centroid = geometry.getCentroid();
			shape.centroid = new ESLocation(centroid.getY(), centroid.getX());
			shape.geoJsonArray = toGeoJsonArray();
			return shape;
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