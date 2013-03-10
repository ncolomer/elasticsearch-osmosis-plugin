package org.openstreetmap.osmosis.plugin.elasticsearch;

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.plugin.elasticsearch.builder.AbstractIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Endpoint;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.EntityBuffer;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.EntityCounter;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Parameters;

public class ElasticSearchWriterTask implements Sink {

	private static final Logger LOG = Logger.getLogger(ElasticSearchWriterTask.class.getName());

	private final Endpoint endpoint;
	private final Set<AbstractIndexBuilder> indexBuilders;
	private final EntityBuffer entityBuffer;
	private final EntityCounter entityCounter;

	public ElasticSearchWriterTask(Endpoint endpoint, Set<AbstractIndexBuilder> indexBuilders, Parameters params) {
		this.endpoint = endpoint;
		this.indexBuilders = indexBuilders;
		int bulkSize = Integer.valueOf(params.getProperty("index.bulk.size", "5000"));
		this.entityBuffer = new EntityBuffer(endpoint.getEntityDao(), bulkSize);
		this.entityCounter = new EntityCounter();
	}

	@Override
	public void initialize(Map<String, Object> metadata) {
		LOG.fine("initialize() with metadata: " + metadata.toString());
	}

	@Override
	public void process(EntityContainer entityContainer) {
		Entity entity = entityContainer.getEntity();
		entityCounter.increment(entity.getType());
		entityBuffer.add(entity);
	}

	@Override
	public void complete() {
		entityBuffer.flush();
		LOG.info("OSM indexing completed!\n" +
				"total processed nodes: ....... " + entityCounter.getCount(EntityType.Node) + "\n" +
				"total processed ways: ........ " + entityCounter.getCount(EntityType.Way) + "\n" +
				"total processed relations: ... " + entityCounter.getCount(EntityType.Relation) + "\n" +
				"total processed bounds: ...... " + entityCounter.getCount(EntityType.Bound));
		endpoint.getIndexAdminService().refresh();
		buildSpecializedIndex();
	}

	protected void buildSpecializedIndex() {
		for (AbstractIndexBuilder indexBuilder : indexBuilders) {
			try {
				String indexName = indexBuilder.getSpecializedIndexName();
				LOG.info("Creating selected index [" + indexName + "]");
				indexBuilder.createIndex();
				LOG.info("Building selected index [" + indexName + "]");
				long time = System.currentTimeMillis();
				indexBuilder.buildIndex();
				time = System.currentTimeMillis() - time;
				LOG.info("Index [" + indexName + "] successfully built in " + time + " milliseconds!");
			} catch (Exception e) {
				LOG.log(Level.SEVERE, "Unable to build index", e);
				break;
			}
		}
	}

	@Override
	public void release() {
		float consumedMemoryMb = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
				/ (float) Math.pow(1024, 2);
		LOG.info(String.format("Estimated memory consumption: %.2f MB", consumedMemoryMb));
		endpoint.getIndexAdminService().refresh();
		endpoint.getClient().close();
	}

}
