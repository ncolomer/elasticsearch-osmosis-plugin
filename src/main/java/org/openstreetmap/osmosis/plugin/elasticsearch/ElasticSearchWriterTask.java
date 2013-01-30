package org.openstreetmap.osmosis.plugin.elasticsearch;

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.index.AbstractIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;

public class ElasticSearchWriterTask implements Sink {

	private static final Logger LOG = Logger.getLogger(ElasticSearchWriterTask.class.getName());

	private int boundProcessedCounter = 0;
	private int nodeProcessedCounter = 0;
	private int wayProcessedCounter = 0;
	private int relationProcessedCounter = 0;

	private final IndexAdminService indexAdminService;
	private final EntityDao entityDao;
	private final Set<AbstractIndexBuilder> indexBuilders;

	public ElasticSearchWriterTask(IndexAdminService indexAdminService, EntityDao entityDao, Set<AbstractIndexBuilder> indexBuilders) {
		this.indexAdminService = indexAdminService;
		this.entityDao = entityDao;
		this.indexBuilders = indexBuilders;
	}

	@Override
	public void initialize(Map<String, Object> metadata) {
		LOG.fine("initialize() with metadata: " + metadata.toString());
	}

	@Override
	public void process(EntityContainer entityContainer) {
		try {
			Entity entity = entityContainer.getEntity();
			entityDao.save(entity);
			switch (entity.getType()) {
			case Node:
				nodeProcessedCounter++;
				break;
			case Way:
				wayProcessedCounter++;
				break;
			case Relation:
				relationProcessedCounter++;
				break;
			case Bound:
				boundProcessedCounter++;
				break;
			}
		} catch (UnsupportedOperationException e) {
			LOG.warning(e.getMessage());
		} catch (Exception e) {
			LOG.severe(e.getMessage());
		}
	}

	@Override
	public void complete() {
		LOG.info("OSM indexing completed!\n" +
				"total processed nodes: ....... " + nodeProcessedCounter + "\n" +
				"total processed ways: ........ " + wayProcessedCounter + "\n" +
				"total processed relations: ... " + relationProcessedCounter + "\n" +
				"total processed bounds: ...... " + boundProcessedCounter);
		indexAdminService.refresh();
		buildSpecializedIndex();
	}

	private void buildSpecializedIndex() {
		for (AbstractIndexBuilder indexBuilder : indexBuilders) {
			try {
				String indexName = indexBuilder.getSpecializedIndexName();
				LOG.info("Creating selected index [" + indexName + "]");
				indexAdminService.createIndex(indexName, indexBuilder.getIndexMapping());
				LOG.info("Building selected index [" + indexName + "]");
				long time = System.currentTimeMillis();
				indexBuilder.buildIndex();
				time = System.currentTimeMillis() - time;
				LOG.info("Index [" + indexName + "] successfully built in " + time + " milliseconds!");
			} catch (Exception e) {
				LOG.log(Level.SEVERE, "Unable to build index", e);
			}
		}
	}

	@Override
	public void release() {
		float consumedMemoryMb = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
				/ (float) Math.pow(1024, 2);
		LOG.info(String.format("Estimated memory consumption: %.2f MB", consumedMemoryMb));
		indexAdminService.refresh();
		indexAdminService.getClient().close();
	}

}
