package org.openstreetmap.osmosis.plugin.elasticsearch.utils;

import java.util.ArrayList;
import java.util.List;

import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;

public class EntityBuffer {

	private final int size;
	private final EntityDao entityDao;
	private final List<Entity> buffer;

	public EntityBuffer(EntityDao entityDao, int size) {
		this.size = size;
		this.buffer = new ArrayList<Entity>(size);
		this.entityDao = entityDao;
	}

	public void add(Entity entity) {
		buffer.add(entity);
		if (buffer.size() == size) flush();
	}

	public void flush() {
		entityDao.saveAll(buffer);
		buffer.clear();
	}

}