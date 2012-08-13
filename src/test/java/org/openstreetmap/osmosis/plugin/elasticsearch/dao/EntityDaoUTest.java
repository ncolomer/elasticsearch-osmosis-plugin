package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import junit.framework.Assert;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.openstreetmap.osmosis.core.domain.v0_6.Bound;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexService;

public class EntityDaoUTest {

	private static final String INDEX_NAME = "osm";

	private IndexService indexServiceMocked;

	private EntityMapper entityMapperMocked;

	private EntityDao entityDao;

	@Before
	public void setUp() throws Exception {
		indexServiceMocked = mock(IndexService.class);
		entityMapperMocked = mock(EntityMapper.class);
		entityDao = new EntityDao(INDEX_NAME, indexServiceMocked);
		entityDao.entityMapper = entityMapperMocked;
	}

	@Test
	public void saveEntity() {
		// Setup
		entityDao = Mockito.spy(entityDao);
		Node node = mock(Node.class);
		Way way = mock(Way.class);
		Relation relation = mock(Relation.class);
		Bound bound = mock(Bound.class);
		doNothing().when(entityDao).saveNode(node);
		doNothing().when(entityDao).saveWay(way);
		doNothing().when(entityDao).saveRelation(relation);
		doNothing().when(entityDao).saveBound(bound);
		when(node.getType()).thenReturn(EntityType.Node);
		when(way.getType()).thenReturn(EntityType.Way);
		when(relation.getType()).thenReturn(EntityType.Relation);
		when(bound.getType()).thenReturn(EntityType.Bound);
		// Action
		entityDao.save((Entity) node);
		entityDao.save((Entity) way);
		entityDao.save((Entity) relation);
		entityDao.save((Entity) bound);
		// Assert
		verify(entityDao, times(1)).saveNode(node);
		verify(entityDao, times(1)).saveWay(way);
		verify(entityDao, times(1)).saveRelation(relation);
		verify(entityDao, times(1)).saveBound(bound);
	}

	@Test
	public void saveNode() throws IOException {
		// Setup
		Node node = mock(Node.class);
		when(node.getType()).thenReturn(EntityType.Node);
		when(node.getId()).thenReturn(1l);
		XContentBuilder content = XContentFactory.jsonBuilder();
		when(entityMapperMocked.marshallNode(node)).thenReturn(content);
		// Action
		entityDao.saveNode(node);
		// Assert
		verify(indexServiceMocked).index(eq(INDEX_NAME),
				eq("node"),
				eq(1l),
				same(content));
	}

	@Test
	public void saveWay() throws IOException {
		// Setup
		Way way = mock(Way.class);
		when(way.getType()).thenReturn(EntityType.Way);
		when(way.getId()).thenReturn(1l);
		XContentBuilder content = XContentFactory.jsonBuilder();
		when(entityMapperMocked.marshallWay(way)).thenReturn(content);
		// Action
		entityDao.saveWay(way);
		// Assert
		verify(indexServiceMocked).index(eq(INDEX_NAME),
				eq("way"),
				eq(1l),
				same(content));
	}

	@Test
	public void saveRelation() {
		// Setup
		Relation relation = mock(Relation.class);
		// Action
		entityDao.saveRelation(relation);
		// Assert
		Mockito.verifyZeroInteractions(indexServiceMocked);
	}

	@Test
	public void saveBound() {
		// Setup
		Bound bound = mock(Bound.class);
		// Action
		entityDao.saveBound(bound);
		// Assert
		Mockito.verifyZeroInteractions(indexServiceMocked);
	}

	@Test
	public void findEntity() {
		// Setup
		entityDao = Mockito.spy(entityDao);
		doReturn(null).when(entityDao).findNode(1l);
		doReturn(null).when(entityDao).findWay(2l);
		doReturn(null).when(entityDao).findRelation(3l);
		doReturn(null).when(entityDao).findBound(4l);
		// Action
		entityDao.find(1l, Node.class);
		entityDao.find(2l, Way.class);
		entityDao.find(3l, Relation.class);
		entityDao.find(4l, Bound.class);
		// Assert
		verify(entityDao, times(1)).findNode(1l);
		verify(entityDao, times(1)).findWay(2l);
		verify(entityDao, times(1)).findRelation(3l);
		verify(entityDao, times(1)).findBound(4l);
	}

	@Test(expected = NullPointerException.class)
	public void findEntity_withNullClass() {
		// Action
		entityDao.find(1l, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void findEntity_withEntityClass() {
		// Action
		entityDao.find(1l, Entity.class);
	}

	@Test
	public void findNode() {
		// Setup
		entityDao = Mockito.spy(entityDao);
		SearchRequestBuilder searchRequestBuilderMocked = mock(SearchRequestBuilder.class);
		SearchResponse searchResponseMocked = mock(SearchResponse.class, Mockito.RETURNS_DEEP_STUBS);
		SearchHit searchHitMocked = mock(SearchHit.class);
		doReturn(searchRequestBuilderMocked).when(entityDao).findNodeQuery(1l);
		when(indexServiceMocked.execute(Mockito.same(searchRequestBuilderMocked))).thenReturn(searchResponseMocked);
		when(searchResponseMocked.getHits().getTotalHits()).thenReturn(1l);
		when(searchResponseMocked.getHits().getAt(0)).thenReturn(searchHitMocked);
		// Action
		entityDao.findNode(1l);
		// Assert
		verify(entityMapperMocked).unmarshallNode(Mockito.same(searchHitMocked));
	}

	@Test
	public void findNode_withNoHit() {
		// Setup
		entityDao = Mockito.spy(entityDao);
		SearchRequestBuilder searchRequestBuilderMocked = mock(SearchRequestBuilder.class);
		SearchResponse searchResponseMocked = mock(SearchResponse.class, Mockito.RETURNS_DEEP_STUBS);
		doReturn(searchRequestBuilderMocked).when(entityDao).findNodeQuery(1l);
		when(indexServiceMocked.execute(Mockito.same(searchRequestBuilderMocked))).thenReturn(searchResponseMocked);
		when(searchResponseMocked.getHits().getTotalHits()).thenReturn(0l);
		// Action
		Node node = entityDao.findNode(1l);
		// Assert
		Assert.assertNull(node);
	}

	@Test
	public void findWay() {
		// Setup
		entityDao = Mockito.spy(entityDao);
		SearchRequestBuilder searchRequestBuilderMocked = mock(SearchRequestBuilder.class);
		SearchResponse searchResponseMocked = mock(SearchResponse.class, Mockito.RETURNS_DEEP_STUBS);
		SearchHit searchHitMocked = mock(SearchHit.class);
		doReturn(searchRequestBuilderMocked).when(entityDao).findWayQuery(1l);
		when(indexServiceMocked.execute(Mockito.same(searchRequestBuilderMocked))).thenReturn(searchResponseMocked);
		when(searchResponseMocked.getHits().getTotalHits()).thenReturn(1l);
		when(searchResponseMocked.getHits().getAt(0)).thenReturn(searchHitMocked);
		// Action
		entityDao.findWay(1l);
		// Assert
		verify(entityMapperMocked).unmarshallWay(Mockito.same(searchHitMocked));
	}

	@Test
	public void findWay_withNoHit() {
		// Setup
		entityDao = Mockito.spy(entityDao);
		SearchRequestBuilder searchRequestBuilderMocked = mock(SearchRequestBuilder.class);
		SearchResponse searchResponseMocked = mock(SearchResponse.class, Mockito.RETURNS_DEEP_STUBS);
		doReturn(searchRequestBuilderMocked).when(entityDao).findWayQuery(1l);
		when(indexServiceMocked.execute(Mockito.same(searchRequestBuilderMocked))).thenReturn(searchResponseMocked);
		when(searchResponseMocked.getHits().getTotalHits()).thenReturn(0l);
		// Action
		Way way = entityDao.findWay(1l);
		// Assert
		Assert.assertNull(way);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void findRelation() {
		entityDao.findRelation(1l);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void findBound() {
		entityDao.findBound(1l);
	}

}
