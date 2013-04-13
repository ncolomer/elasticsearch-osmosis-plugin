package org.openstreetmap.osmosis.plugin.elasticsearch.model.shape;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.geo.GeoShapeConstants;

import com.spatial4j.core.distance.DistanceUtils;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;

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
			ESShape esShape = new ESShape();
			esShape.isClosed = isClosed();
			esShape.esShapeType = getShapeType();
			Geometry geometry = getGeometry();
			esShape.area = degree2ToKm2(geometry.getArea());
			esShape.lenght = degreeToKm(geometry.getLength());
			Point centroid = geometry.getCentroid();
			esShape.centroid = new ESLocation(centroid.getY(), centroid.getX());
			esShape.geoJsonArray = toGeoJsonArray();
			return esShape;
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

		private Geometry getGeometry() {
			Coordinate[] coordinates = new Coordinate[locations.size()];
			for (int i = 0; i < locations.size(); i++) {
				coordinates[i] = new Coordinate(
						locations.get(i).getLongitude(),
						locations.get(i).getLatitude());
			}
			GeometryFactory geometryFactory = GeoShapeConstants.SPATIAL_CONTEXT.getGeometryFactory();
			switch (getShapeType()) {
			case POINT:
				return geometryFactory.createPoint(coordinates[0]);
			case LINESTRING:
				return geometryFactory.createLineString(coordinates);
			case POLYGON:
				LinearRing shell = geometryFactory.createLinearRing(coordinates);
				return geometryFactory.createPolygon(shell, null);
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