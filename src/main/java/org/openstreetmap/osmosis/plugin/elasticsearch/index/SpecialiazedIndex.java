package org.openstreetmap.osmosis.plugin.elasticsearch.index;

import org.openstreetmap.osmosis.plugin.elasticsearch.index.rg.RgIndexBuilder;

public enum SpecialiazedIndex {

	RG_INDEX(RgIndexBuilder.class);

	private final Class<? extends IndexBuilder> clazz;

	private SpecialiazedIndex(Class<? extends IndexBuilder> clazz) {
		this.clazz = clazz;
	}

	public Class<? extends IndexBuilder> getIndexBuilderClass() {
		return this.clazz;
	}

}
