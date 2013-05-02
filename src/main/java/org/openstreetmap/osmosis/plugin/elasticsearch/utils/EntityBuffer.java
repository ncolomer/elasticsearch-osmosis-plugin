package org.openstreetmap.osmosis.plugin.elasticsearch.utils;

import java.util.ArrayList;
import java.util.List;

import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;

public class EntityBuffer {

	private final int size;
	private final EntityDao entityDao;
	private final List<Entity> buffer;

	private EntityBuffer(EntityDao entityDao, int size) {
		this.size = size;
		this.buffer = new ArrayList<Entity>(size);
		this.entityDao = entityDao;
	}

	public boolean add(Entity entity) {
		buffer.add(entity);
		if (buffer.size() == size) {
			flush();
			return true;
		} else return false;
	}

	public void flush() {
		entityDao.saveAll(buffer);
		buffer.clear();
	}

	public static class EntityBufferFactory {

		private final EntityDao entityDao;
		private final int nodeBulkSize;
		private final int wayBulkSize;

		public EntityBufferFactory(EntityDao entityDao, Parameters params) {
			this.entityDao = entityDao;
			this.nodeBulkSize = Integer.valueOf(params.getProperty(Parameters.CONFIG_NODE_BULK_SIZE));
			this.wayBulkSize = Integer.valueOf(params.getProperty(Parameters.CONFIG_WAY_BULK_SIZE));
		}

		public EntityBuffer buildForType(EntityType type) {
			switch (type) {
			case Node:
				return new EntityBuffer(entityDao, nodeBulkSize);
			case Way:
				return new EntityBuffer(entityDao, wayBulkSize);
			case Relation:
			case Bound:
			default:
				return new EntityBuffer(entityDao, 10);
			}

		}

	}

}