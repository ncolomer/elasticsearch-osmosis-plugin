package org.openstreetmap.osmosis.plugin.elasticsearch.model.shape;

public class ESLocation {

	private final double latitude;
	private final double longitude;

	public ESLocation(double latitude, double longitude) {
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public ESLocation(double[] geoJsonArray) {
		if (geoJsonArray.length != 2) throw new IllegalArgumentException("Invalid array");
		this.latitude = geoJsonArray[1];
		this.longitude = geoJsonArray[0];
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public double[] toGeoJsonArray() {
		return new double[] { longitude, latitude };
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		ESLocation other = (ESLocation) obj;
		if (Double.doubleToLongBits(latitude) != Double.doubleToLongBits(other.latitude)) return false;
		if (Double.doubleToLongBits(longitude) != Double.doubleToLongBits(other.longitude)) return false;
		return true;
	}

	@Override
	public String toString() {
		return "Location [latitude=" + latitude + ", longitude=" + longitude + "]";
	}

}
