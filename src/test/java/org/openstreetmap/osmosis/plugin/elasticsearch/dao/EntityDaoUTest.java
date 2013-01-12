package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.openstreetmap.osmosis.core.domain.v0_6.Bound;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.OsmDataBuilder;

@SuppressWarnings("unchecked")
public class EntityDaoUTest {

	private static final String INDEX_NAME = "osm-test";

	private Client clientMocked;

	private EntityMapper entityMapperMocked;

	private EntityDao entityDao;

	@Before
	public void setUp() throws Exception {
		clientMocked = mock(Client.class);
		entityMapperMocked = mock(EntityMapper.class);
		entityDao = new EntityDao(INDEX_NAME, clientMocked);
		entityDao.entityMapper = entityMapperMocked;
		entityDao = Mockito.spy(entityDao);
	}

	@Test
	public void saveEntity() {
		// Setup
		Node node = mock(Node.class);
		Way way = mock(Way.class);
		Relation relation = mock(Relation.class);
		Bound bound = mock(Bound.class);

		when(node.getType()).thenReturn(EntityType.Node);
		when(way.getType()).thenReturn(EntityType.Way);
		when(relation.getType()).thenReturn(EntityType.Relation);
		when(bound.getType()).thenReturn(EntityType.Bound);

		doReturn("1").when(entityDao).saveNode(node);
		doReturn("2").when(entityDao).saveWay(way);
		doReturn("3").when(entityDao).saveRelation(relation);
		doReturn("4").when(entityDao).saveBound(bound);

		// Action
		String nodeId = entityDao.save((Entity) node);
		String wayId = entityDao.save((Entity) way);
		String relationId = entityDao.save((Entity) relation);
		String boundId = entityDao.save((Entity) bound);

		// Assert
		verify(entityDao, times(1)).saveNode(node);
		verify(entityDao, times(1)).saveWay(way);
		verify(entityDao, times(1)).saveRelation(relation);
		verify(entityDao, times(1)).saveBound(bound);

		assertEquals("1", nodeId);
		assertEquals("2", wayId);
		assertEquals("3", relationId);
		assertEquals("4", boundId);
	}

	@Test(expected = IllegalArgumentException.class)
	public void saveEntity_withNull_shouldThrowException() {
		// Action
		entityDao.save(null);
	}

	@Test
	public void saveNode() throws IOException {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();
		XContentBuilder xContentBuilder = new EntityMapper().marshallNode(node);

		IndexRequestBuilder indexRequestBuilderMocked = mock(IndexRequestBuilder.class);
		ListenableActionFuture<IndexResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		IndexResponse indexResponseMocked = mock(IndexResponse.class);
		when(clientMocked.prepareIndex(any(String.class), any(String.class), any(String.class))).thenReturn(indexRequestBuilderMocked);
		when(entityMapperMocked.marshallNode(node)).thenReturn(xContentBuilder);
		when(indexRequestBuilderMocked.setSource(any(XContentBuilder.class))).thenReturn(indexRequestBuilderMocked);
		when(indexRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenReturn(indexResponseMocked);
		when(indexResponseMocked.getId()).thenReturn("1");

		// Action
		String id = entityDao.saveNode(node);

		// Assert
		assertEquals("1", id);
		verify(clientMocked).prepareIndex(INDEX_NAME, "node", "1");
		verify(entityMapperMocked).marshallNode(node);
		verify(indexRequestBuilderMocked).setSource(xContentBuilder);
	}

	@Test(expected = DaoException.class)
	public void saveNode_withClientException_shouldThrowDaoException() throws IOException {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();
		when(clientMocked.prepareIndex(any(String.class), any(String.class), any(String.class))).thenThrow(new ElasticSearchException("Simulated Exception"));

		// Action
		entityDao.saveNode(node);
	}

	@Test
	public void saveWay() throws IOException {
		// Setup
		Way way = OsmDataBuilder.buildSampleWay();
		XContentBuilder xContentBuilder = new EntityMapper().marshallWay(way);

		IndexRequestBuilder indexRequestBuilderMocked = mock(IndexRequestBuilder.class);
		ListenableActionFuture<IndexResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		IndexResponse indexResponseMocked = mock(IndexResponse.class);
		when(clientMocked.prepareIndex(any(String.class), any(String.class), any(String.class))).thenReturn(indexRequestBuilderMocked);
		when(entityMapperMocked.marshallWay(way)).thenReturn(xContentBuilder);
		when(indexRequestBuilderMocked.setSource(any(XContentBuilder.class))).thenReturn(indexRequestBuilderMocked);
		when(indexRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenReturn(indexResponseMocked);
		when(indexResponseMocked.getId()).thenReturn("1");

		// Action
		String id = entityDao.saveWay(way);

		// Assert
		assertEquals("1", id);
		verify(clientMocked).prepareIndex(INDEX_NAME, "way", "1");
		verify(indexRequestBuilderMocked).setSource(xContentBuilder);
	}

	@Test(expected = DaoException.class)
	public void saveWay_withClientException_shouldThrowDaoException() throws IOException {
		// Setup
		Way way = OsmDataBuilder.buildSampleWay();
		when(clientMocked.prepareIndex(any(String.class), any(String.class), any(String.class))).thenThrow(new ElasticSearchException("Simulated Exception"));

		// Action
		entityDao.saveWay(way);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void saveRelation_shouldBeUnsupported() {
		// Setup
		Relation relation = mock(Relation.class);

		// Action
		entityDao.saveRelation(relation);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void saveBound_shouldBeUnsupported() {
		// Setup
		Bound bound = mock(Bound.class);

		// Action
		entityDao.saveBound(bound);
	}

	@Test
	public void findEntity() {
		// Setup
		Node node = mock(Node.class);
		Way way = mock(Way.class);
		Relation relation = mock(Relation.class);
		Bound bound = mock(Bound.class);

		doReturn(node).when(entityDao).findNode(1l);
		doReturn(way).when(entityDao).findWay(2l);
		doReturn(relation).when(entityDao).findRelation(3l);
		doReturn(bound).when(entityDao).findBound(4l);

		// Action
		Node resultNode = entityDao.find(1l, Node.class);
		Way resultWay = entityDao.find(2l, Way.class);
		Relation resultRelation = entityDao.find(3l, Relation.class);
		Bound resultBound = entityDao.find(4l, Bound.class);

		// Assert
		verify(entityDao, times(1)).findNode(1l);
		verify(entityDao, times(1)).findWay(2l);
		verify(entityDao, times(1)).findRelation(3l);
		verify(entityDao, times(1)).findBound(4l);

		assertSame(node, resultNode);
		assertSame(way, resultWay);
		assertSame(relation, resultRelation);
		assertSame(bound, resultBound);
	}

	@Test(expected = IllegalArgumentException.class)
	public void findEntity_withNullClass_shouldThrowException() {
		// Action
		entityDao.find(1l, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void findEntity_withEntityClass_shouldThrowException() {
		// Action
		entityDao.find(1l, Entity.class);
	}

	@Test
	public void findNode() {
		// Setup
		Node expected = OsmDataBuilder.buildSampleNode();

		SearchRequestBuilder searchRequestBuilderMocked = mock(SearchRequestBuilder.class);
		ListenableActionFuture<SearchResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		SearchResponse searchResponseMocked = mock(SearchResponse.class);
		SearchHits searchHitsMocked = mock(SearchHits.class);
		SearchHit searchHitMocked = mock(SearchHit.class);

		when(clientMocked.prepareSearch(any(String.class))).thenReturn(searchRequestBuilderMocked);
		when(searchRequestBuilderMocked.setQuery(any(QueryBuilder.class))).thenReturn(searchRequestBuilderMocked);
		when(searchRequestBuilderMocked.addFields(any(String.class), any(String.class))).thenReturn(searchRequestBuilderMocked);
		when(searchRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenReturn(searchResponseMocked);
		when(searchResponseMocked.getHits()).thenReturn(searchHitsMocked);
		when(searchHitsMocked.getTotalHits()).thenReturn(1l);
		when(searchHitsMocked.getAt(0)).thenReturn(searchHitMocked);

		when(entityMapperMocked.unmarshallNode(any(SearchHit.class))).thenReturn(expected);

		// Action
		Node actual = entityDao.findNode(1l);

		// Assert
		verify(searchRequestBuilderMocked).setQuery(argThat(new QueryBuilderMatcher(QueryBuilders.idsQuery("node").ids("1"))));
		verify(searchRequestBuilderMocked).addFields("location", "tags");
		verify(entityMapperMocked).unmarshallNode(searchHitMocked);
		assertEquals(expected, actual);
	}

	@Test
	public void findNode_withNoHit() {
		// Setup
		SearchRequestBuilder searchRequestBuilderMocked = mock(SearchRequestBuilder.class);
		ListenableActionFuture<SearchResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		SearchResponse searchResponseMocked = mock(SearchResponse.class);
		SearchHits searchHitsMocked = mock(SearchHits.class);

		when(clientMocked.prepareSearch(any(String.class))).thenReturn(searchRequestBuilderMocked);
		when(searchRequestBuilderMocked.setQuery(any(QueryBuilder.class))).thenReturn(searchRequestBuilderMocked);
		when(searchRequestBuilderMocked.addFields(any(String.class), any(String.class))).thenReturn(searchRequestBuilderMocked);
		when(searchRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenReturn(searchResponseMocked);
		when(searchResponseMocked.getHits()).thenReturn(searchHitsMocked);
		when(searchHitsMocked.getTotalHits()).thenReturn(0l);

		// Action
		Node actual = entityDao.findNode(1l);

		// Assert
		verify(searchRequestBuilderMocked).setQuery(argThat(new QueryBuilderMatcher(QueryBuilders.idsQuery("node").ids("1"))));
		verify(searchRequestBuilderMocked).addFields("location", "tags");
		assertNull(actual);
	}

	@Test(expected = DaoException.class)
	public void findNode_withClientException_shouldThrowDaoException() throws IOException {
		// Setup
		when(clientMocked.prepareSearch(any(String.class))).thenThrow(new ElasticSearchException("Simulated Exception"));

		// Action
		entityDao.findNode(1l);
	}

	@Test
	public void findWay() {
		// Setup
		Way expected = OsmDataBuilder.buildSampleWay();

		SearchRequestBuilder searchRequestBuilderMocked = mock(SearchRequestBuilder.class);
		ListenableActionFuture<SearchResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		SearchResponse searchResponseMocked = mock(SearchResponse.class);
		SearchHits searchHitsMocked = mock(SearchHits.class);
		SearchHit searchHitMocked = mock(SearchHit.class);

		when(clientMocked.prepareSearch(any(String.class))).thenReturn(searchRequestBuilderMocked);
		when(searchRequestBuilderMocked.setQuery(any(QueryBuilder.class))).thenReturn(searchRequestBuilderMocked);
		when(searchRequestBuilderMocked.addFields(any(String.class), any(String.class))).thenReturn(searchRequestBuilderMocked);
		when(searchRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenReturn(searchResponseMocked);
		when(searchResponseMocked.getHits()).thenReturn(searchHitsMocked);
		when(searchHitsMocked.getTotalHits()).thenReturn(1l);
		when(searchHitsMocked.getAt(0)).thenReturn(searchHitMocked);

		when(entityMapperMocked.unmarshallWay(any(SearchHit.class))).thenReturn(expected);

		// Action
		Way actual = entityDao.findWay(1l);

		// Assert
		verify(searchRequestBuilderMocked).setQuery(argThat(new QueryBuilderMatcher(QueryBuilders.idsQuery("way").ids("1"))));
		verify(searchRequestBuilderMocked).addFields("tags", "nodes");
		verify(entityMapperMocked).unmarshallWay(searchHitMocked);
		assertEquals(expected, actual);
	}

	@Test
	public void findWay_withNoHit() {
		// Setup
		SearchRequestBuilder searchRequestBuilderMocked = mock(SearchRequestBuilder.class);
		ListenableActionFuture<SearchResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		SearchResponse searchResponseMocked = mock(SearchResponse.class);
		SearchHits searchHitsMocked = mock(SearchHits.class);

		when(clientMocked.prepareSearch(any(String.class))).thenReturn(searchRequestBuilderMocked);
		when(searchRequestBuilderMocked.setQuery(any(QueryBuilder.class))).thenReturn(searchRequestBuilderMocked);
		when(searchRequestBuilderMocked.addFields(any(String.class), any(String.class))).thenReturn(searchRequestBuilderMocked);
		when(searchRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenReturn(searchResponseMocked);
		when(searchResponseMocked.getHits()).thenReturn(searchHitsMocked);
		when(searchHitsMocked.getTotalHits()).thenReturn(0l);

		// Action
		Way actual = entityDao.findWay(1l);

		// Assert
		verify(searchRequestBuilderMocked).setQuery(argThat(new QueryBuilderMatcher(QueryBuilders.idsQuery("way").ids("1"))));
		verify(searchRequestBuilderMocked).addFields("tags", "nodes");
		assertNull(actual);
	}

	@Test(expected = DaoException.class)
	public void findWay_withClientException_shouldThrowDaoException() throws IOException {
		// Setup
		when(clientMocked.prepareSearch(any(String.class))).thenThrow(new ElasticSearchException("Simulated Exception"));

		// Action
		entityDao.findWay(1l);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void findRelation_shouldBeUnsupported() {
		// Action
		entityDao.find(1l, Relation.class);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void findBound_shouldBeUnsupported() {
		// Action
		entityDao.find(1l, Bound.class);
	}

	@Test
	public void delete() {
		// Setup
		doReturn(true).when(entityDao).deleteEntity(1l, "node");
		doReturn(true).when(entityDao).deleteEntity(2l, "way");

		// Action
		boolean resultNode = entityDao.delete(1l, Node.class);
		boolean resultWay = entityDao.delete(2l, Way.class);

		// Assert
		verify(entityDao, times(1)).deleteEntity(1l, "node");
		verify(entityDao, times(1)).deleteEntity(2l, "way");
		assertEquals(true, resultNode);
		assertEquals(true, resultWay);
	}

	@Test(expected = IllegalArgumentException.class)
	public void delete_withNullClass_shouldThrowException() {
		// Action
		entityDao.delete(1l, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void delete_withEntityClass_shouldThrowException() {
		// Action
		entityDao.delete(1l, Entity.class);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void deleteRelation_shouldBeUnsupported() {
		// Action
		entityDao.delete(1l, Relation.class);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void deleteBound_shouldBeUnsupported() {
		// Action
		entityDao.delete(1l, Bound.class);
	}

	@Test
	public void deleteEntity() {
		// Setup
		DeleteRequestBuilder deleteRequestBuilderMocked = mock(DeleteRequestBuilder.class);
		ListenableActionFuture<DeleteResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		DeleteResponse deleteResponseMocked = mock(DeleteResponse.class);

		when(clientMocked.prepareDelete(any(String.class), any(String.class), any(String.class))).thenReturn(deleteRequestBuilderMocked);
		when(deleteRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenReturn(deleteResponseMocked);
		when(deleteResponseMocked.notFound()).thenReturn(false);

		// Action
		boolean actual = entityDao.deleteEntity(1l, "node");

		// Assert
		verify(clientMocked).prepareDelete(INDEX_NAME, "node", "1");
		assertEquals(true, actual);
	}

	@Test
	public void deleteEntity_withEntityNotFound() {
		// Setup
		DeleteRequestBuilder deleteRequestBuilderMocked = mock(DeleteRequestBuilder.class);
		ListenableActionFuture<DeleteResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		DeleteResponse deleteResponseMocked = mock(DeleteResponse.class);

		when(clientMocked.prepareDelete(any(String.class), any(String.class), any(String.class))).thenReturn(deleteRequestBuilderMocked);
		when(deleteRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenReturn(deleteResponseMocked);
		when(deleteResponseMocked.notFound()).thenReturn(true);

		// Action
		boolean actual = entityDao.deleteEntity(1l, "node");

		// Assert
		verify(clientMocked).prepareDelete(INDEX_NAME, "node", "1");
		assertEquals(false, actual);
	}

	@Test(expected = DaoException.class)
	public void deleteEntity_withClientException_shouldThrowDaoException() throws IOException {
		// Setup
		when(clientMocked.prepareDelete()).thenThrow(new ElasticSearchException("Simulated Exception"));

		// Action
		entityDao.deleteEntity(1l, "node");
	}

	public class QueryBuilderMatcher extends BaseMatcher<QueryBuilder> {

		private final QueryBuilder expected;

		public QueryBuilderMatcher(QueryBuilder expected) {
			this.expected = expected;
		}

		@Override
		public boolean matches(Object item) {
			if (expected == item) return true;
			if (item == null) return false;
			if (expected.getClass() != item.getClass()) return false;
			QueryBuilder other = (QueryBuilder) item;
			return expected.toString().equals(other.toString());
		}

		@Override
		public void describeTo(Description description) {}

	}

}
