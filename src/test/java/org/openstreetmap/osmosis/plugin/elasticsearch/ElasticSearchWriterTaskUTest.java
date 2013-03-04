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
import java.util.Properties;
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

public class ElasticSearchWriterTaskUTest {

	private IndexAdminService indexAdminServiceMocked;
	private Set<AbstractIndexBuilder> indexBuilders;
	private ElasticSearchWriterTask elasticSearchWriterTask;
	private EntityDao entityDaoMocked;

	@Before
	public void setUp() throws Exception {
		indexAdminServiceMocked = mock(IndexAdminService.class);
		indexBuilders = new HashSet<AbstractIndexBuilder>();
		entityDaoMocked = mock(EntityDao.class);
		Properties params = new Properties();
		params.setProperty("index.bulk.size", "1");
		elasticSearchWriterTask = new ElasticSearchWriterTask(indexAdminServiceMocked, entityDaoMocked, indexBuilders, params);
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
		// Setup
		Client clientMocked = mock(Client.class);
		when(indexAdminServiceMocked.getClient()).thenReturn(clientMocked);

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
