package org.openstreetmap.osmosis.plugin.elasticsearch;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.index.SpecialiazedIndex;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;

public class ElasticSearchWriterTaskTest {

	private IndexAdminService indexAdminServiceMocked;
	private EntityDao entityDaoMocked;
	private Set<SpecialiazedIndex> indexSet;
	private ElasticSearchWriterTask elasticSearchWriterTask;

	@Before
	public void setUp() throws Exception {
		indexAdminServiceMocked = mock(IndexAdminService.class);
		entityDaoMocked = mock(EntityDao.class);
		indexSet = new HashSet<SpecialiazedIndex>();
		elasticSearchWriterTask = new ElasticSearchWriterTask(indexAdminServiceMocked, entityDaoMocked, indexSet);
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

}
