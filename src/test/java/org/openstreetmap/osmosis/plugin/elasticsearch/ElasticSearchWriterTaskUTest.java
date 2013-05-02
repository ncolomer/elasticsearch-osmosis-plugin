package org.openstreetmap.osmosis.plugin.elasticsearch;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.builder.AbstractIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Endpoint;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Parameters;

public class ElasticSearchWriterTaskUTest {

	private Client clientMocked;
	private IndexAdminService indexAdminServiceMocked;
	private EntityDao entityDaoMocked;
	private Endpoint endpoint;
	private Set<AbstractIndexBuilder> indexBuilders;
	private Parameters params;

	private ElasticSearchWriterTask elasticSearchWriterTask;

	@Before
	public void setUp() throws Exception {
		clientMocked = mock(Client.class);
		indexAdminServiceMocked = mock(IndexAdminService.class);
		entityDaoMocked = mock(EntityDao.class);
		endpoint = new Endpoint(clientMocked, indexAdminServiceMocked, entityDaoMocked);
		indexBuilders = new HashSet<AbstractIndexBuilder>();
		params = new Parameters.Builder().loadResource("plugin.properties")
				.addParameter(Parameters.CONFIG_QUEUE_SIZE, "1")
				.addParameter(Parameters.CONFIG_NODE_BULK_SIZE, "1")
				.addParameter(Parameters.CONFIG_WAY_BULK_SIZE, "1")
				.addParameter(Parameters.CONFIG_WORKER_POOL_SIZE, "1").build();
		elasticSearchWriterTask = spy(new ElasticSearchWriterTask(endpoint, indexBuilders, params));
	}

	@Test
	public void process() {
		// Setup
		Entity entityMocked = mock(Entity.class);
		when(entityMocked.getType()).thenReturn(EntityType.Node);

		EntityContainer entityContainerMocked = mock(EntityContainer.class);
		when(entityContainerMocked.getEntity()).thenReturn(entityMocked);

		// Action
		elasticSearchWriterTask.process(entityContainerMocked);
		elasticSearchWriterTask.complete();

		// Assert
		verify(entityDaoMocked, times(2)).saveAll(eq(Arrays.asList(new Entity[] {})));
	}

	@Test
	public void complete() {
		// Setup
		doNothing().when(elasticSearchWriterTask).buildSpecializedIndex();

		// Action
		elasticSearchWriterTask.complete();

		// Assert
		verify(elasticSearchWriterTask, times(1)).buildSpecializedIndex();
	}

	@Test
	public void release() {
		// Action
		elasticSearchWriterTask.release();

		// Assert
		verify(clientMocked, times(1)).close();
	}

	public class DummyIndexBuilder extends AbstractIndexBuilder {

		public DummyIndexBuilder(Endpoint endpoint, Parameters params) {
			super(endpoint, params);
		}

		@Override
		public String getSpecializedIndexSuffix() {
			return "dummy";
		}

		@Override
		public void createIndex() {}

		@Override
		public void buildIndex() {}
	}

}
