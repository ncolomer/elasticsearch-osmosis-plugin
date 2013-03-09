package org.openstreetmap.osmosis.plugin.elasticsearch;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
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

	private ElasticSearchWriterTask elasticSearchWriterTask;

	@Before
	public void setUp() throws Exception {
		clientMocked = mock(Client.class);
		indexAdminServiceMocked = mock(IndexAdminService.class);
		entityDaoMocked = mock(EntityDao.class);
		endpoint = new Endpoint(clientMocked, indexAdminServiceMocked, entityDaoMocked);
		indexBuilders = new HashSet<AbstractIndexBuilder>();
		Parameters params = new Parameters.Builder().loadResource("plugin.properties")
				.addParameter("index.bulk.size", "1").build();
		elasticSearchWriterTask = new ElasticSearchWriterTask(endpoint, indexBuilders, params);
	}

	@Test
	public void process() {
		// Setup
		Entity entityMocked = mock(Entity.class);
		EntityContainer entityContainerMocked = mock(EntityContainer.class);
		when(entityContainerMocked.getEntity()).thenReturn(entityMocked);
		when(entityMocked.getType()).thenReturn(EntityType.Node);

		// Action
		elasticSearchWriterTask.process(entityContainerMocked);

		// Assert
		verify(entityDaoMocked, times(1)).saveAll(eq(Arrays.asList(new Entity[] {})));
	}

	@Test
	public void complete() {
		// Setup
		AbstractIndexBuilder indexBuilderMocked = spy(new DummyIndexBuilder());
		indexBuilders.add(indexBuilderMocked);

		// Action
		elasticSearchWriterTask.complete();

		// Assert
		verify(entityDaoMocked, times(1)).saveAll(eq(new ArrayList<Entity>()));
		verify(indexBuilderMocked, times(1)).getSpecializedIndexName();
		verify(indexBuilderMocked, times(1)).getIndexConfig();
		verify(indexAdminServiceMocked, times(1)).createIndex(eq("index-test"), eq("{}"));
		verify(indexBuilderMocked, times(1)).buildIndex();
	}

	@Test
	public void release() {
		// Action
		elasticSearchWriterTask.release();

		// Assert
		verify(indexAdminServiceMocked, times(1)).refresh();
		verify(clientMocked, times(1)).close();
	}

	public class DummyIndexBuilder extends AbstractIndexBuilder {

		public DummyIndexBuilder() {
			super(null, null, "index", "{}");
		}

		@Override
		public String getSpecializedIndexSuffix() {
			return "test";
		}

		@Override
		public void buildIndex() {}
	}

}
