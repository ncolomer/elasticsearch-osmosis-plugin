package org.openstreetmap.osmosis.plugin.elasticsearch.index;

import org.openstreetmap.osmosis.plugin.elasticsearch.index.rg.RgIndexBuilder;

public enum SpecialiazedIndex {

	RG_INDEX(RgIndexBuilder.class);

	private final Class<? extends AbstractIndexBuilder> clazz;

	private SpecialiazedIndex(Class<? extends AbstractIndexBuilder> clazz) {
		this.clazz = clazz;
	}

	public Class<? extends AbstractIndexBuilder> getIndexBuilderClass() {
		return this.clazz;
	}

}
