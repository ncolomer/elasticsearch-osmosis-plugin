package org.openstreetmap.osmosis.plugin.elasticsearch;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.index.AbstractIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;

public class ElasticSearchWriterTaskUTest {

	private IndexAdminService indexAdminServiceMocked;
	private EntityDao entityDaoMocked;
	private Set<AbstractIndexBuilder> indexBuilders;
	private ElasticSearchWriterTask elasticSearchWriterTask;

	@Before
	public void setUp() throws Exception {
		indexAdminServiceMocked = mock(IndexAdminService.class);
		entityDaoMocked = mock(EntityDao.class);
		indexBuilders = new HashSet<AbstractIndexBuilder>();
		elasticSearchWriterTask = new ElasticSearchWriterTask(indexAdminServiceMocked, entityDaoMocked, indexBuilders);
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
		verify(entityDaoMocked, times(1)).save(entityMocked);
	}

	@Test
	public void complete() {
		// Setup
		AbstractIndexBuilder indexBuilderMocked = spy(new DummyIndexBuilder());
		indexBuilders.add(indexBuilderMocked);

		// Action
		elasticSearchWriterTask.complete();

		// Assert
		verify(indexBuilderMocked, times(1)).getSpecializedIndexName();
		verify(indexBuilderMocked, times(1)).getIndexMapping();
		Map<String, XContentBuilder> map = new HashMap<String, XContentBuilder>();
		verify(indexAdminServiceMocked, times(1)).createIndex(eq("index-test"), eq(map));
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
			super(null, null, "index");
		}

		@Override
		public String getSpecializedIndexSuffix() {
			return "test";
		}

		@Override
		public Map<String, XContentBuilder> getIndexMapping() {
			return new HashMap<String, XContentBuilder>();
		}

		@Override
		public void buildIndex() {}
	}

}
