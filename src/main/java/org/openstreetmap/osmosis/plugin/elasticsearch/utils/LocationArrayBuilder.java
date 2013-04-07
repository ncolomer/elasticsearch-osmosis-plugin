package org.openstreetmap.osmosis.plugin.elasticsearch.utils;

import java.util.ArrayList;
import java.util.List;

public class LocationArrayBuilder {

	private List<Location> locations;

	public LocationArrayBuilder() {
		this.locations = new ArrayList<Location>();
	}

	public LocationArrayBuilder(int size) {
		this.locations = new ArrayList<Location>(size);
	}

	public LocationArrayBuilder addLocation(double latitude, double longitude) {
		locations.add(new Location(latitude, longitude));
		return this;
	}

	public double[][] toArray() {
		double[][] array = new double[locations.size()][2];
		for (int i = 0; i < locations.size(); i++) {
			array[i] = new double[] { getAt(i)[0], getAt(i)[1] };
		}
		return array;
	}

	public double[] getAt(int index) {
		Location location = locations.get(index);
		// GeoJSON Point coordinates: [longitude (x), latitude (y)]
		return new double[] { location.longitude, location.latitude };
	}

	public static class Location {

		private final double latitude;
		private final double longitude;

		public Location(double latitude, double longitude) {
			this.latitude = latitude;
			this.longitude = longitude;
		}

		public double getLatitude() {
			return latitude;
		}

		public double getLongitude() {
			return longitude;
		}

	}

}