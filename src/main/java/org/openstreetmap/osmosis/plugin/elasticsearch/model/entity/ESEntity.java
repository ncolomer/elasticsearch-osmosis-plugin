package org.openstreetmap.osmosis.plugin.elasticsearch.model.entity;

import java.util.HashMap;
import java.util.Map;

import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESLocation;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESShapeType;

public abstract class ESEntity {

	private final long id;
	private final Map<String, String> tags;

	protected ESEntity(Entity entity) {
		this.id = entity.getId();
		this.tags = new HashMap<String, String>();
		for (Tag tag : entity.getTags()) {
			this.tags.put(tag.getKey(), tag.getValue());
		}
	}

	protected ESEntity(long id, Map<String, String> tags) {
		this.id = id;
		this.tags = tags;
	}

	public abstract ESEntityType getEntityType();

	public abstract ESShapeType getShapeType();

	public abstract ESLocation getCentroid();

	public abstract double getLenght();

	public abstract double getArea();

	public abstract String toJson();

	public long getId() {
		return id;
	}

	public String getIdString() {
		return Long.toString(id);
	}

	public Map<String, String> getTags() {
		return tags;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + ((tags == null) ? 0 : tags.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		ESEntity other = (ESEntity) obj;
		if (id != other.id) return false;
		if (tags == null) {
			if (other.tags != null) return false;
		} else if (!tags.equals(other.tags)) return false;
		return true;
	}

}
