package org.openstreetmap.osmosis.plugin.elasticsearch.model;

public enum ESEntityType {

	BOUND, NODE, WAY, RELATION;

	public String getIndiceName() {
		return this.name().toLowerCase();
	}

	public static <T extends ESEntity> ESEntityType valueOf(Class<T> entityClass) {
		if (entityClass == null) throw new IllegalArgumentException("Provided Entity class is null");
		else if (entityClass.equals(ESNode.class)) return NODE;
		else if (entityClass.equals(ESWay.class)) return WAY;
		else throw new IllegalArgumentException(entityClass.getSimpleName() + " is not a valid Entity");
	}

}
