package org.openstreetmap.osmosis.plugin.elasticsearch.utils;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;

public class EntityCounter {

	private final Map<EntityType, AtomicInteger> counters;

	public EntityCounter() {
		counters = new EnumMap<EntityType, AtomicInteger>(EntityType.class);
		for (EntityType type : EntityType.values()) {
			counters.put(type, new AtomicInteger());
		}
	}

	public void increment(EntityType type) {
		counters.get(type).incrementAndGet();
	}

	public int getCount(EntityType type) {
		return counters.get(type).get();
	}

}