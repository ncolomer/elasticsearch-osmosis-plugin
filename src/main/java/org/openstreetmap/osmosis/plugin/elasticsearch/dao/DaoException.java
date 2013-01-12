package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

public class DaoException extends RuntimeException {

	private static final long serialVersionUID = 6120808051925518379L;

	public DaoException() {
		super();
	}

	public DaoException(String message) {
		super(message);
	}

	public DaoException(Throwable throwable) {
		super(throwable);
	}

	public DaoException(String message, Throwable throwable) {
		super(message, throwable);
	}

}