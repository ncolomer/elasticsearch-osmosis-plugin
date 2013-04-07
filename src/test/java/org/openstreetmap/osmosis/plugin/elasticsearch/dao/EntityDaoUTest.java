package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
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
import org.openstreetmap.osmosis.plugin.elasticsearch.model.ESEntity;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.ESEntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.ESNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.OsmDataBuilder;

@SuppressWarnings("unchecked")
public class EntityDaoUTest {

	private static final String INDEX_NAME = "osm-test";

	private Client clientMocked;

	private EntityDao entityDao;

	@Before
	public void setUp() throws Exception {
		clientMocked = mock(Client.class);
		entityDao = new EntityDao(INDEX_NAME, clientMocked);
		entityDao = Mockito.spy(entityDao);
	}

	/* SAVE */

	@Test
	public void saveEntity() throws Exception {
		// Setup
		Entity entity = mock(Entity.class);
		IndexRequestBuilder indexRequestBuilderMocked = mock(IndexRequestBuilder.class);
		ListenableActionFuture<IndexResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		IndexResponse indexResponseMocked = mock(IndexResponse.class);

		doReturn(indexRequestBuilderMocked).when(entityDao).buildIndexRequest(entity);
		when(indexRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenReturn(indexResponseMocked);

		// Action
		entityDao.save(entity);

		// Assert
		verify(entityDao, times(1)).buildIndexRequest(entity);
		verify(indexRequestBuilderMocked, times(1)).execute();
		verify(listenableActionFutureMocked, times(1)).actionGet();
	}

	@Test(expected = IllegalArgumentException.class)
	public void saveEntity_withNull_shouldThrowException() {
		// Action
		entityDao.save(null);
	}

	@Test(expected = DaoException.class)
	public void saveEntity_withRelation_shouldBeUnsupported() {
		// Setup
		Relation relation = mock(Relation.class);
		when(relation.getType()).thenReturn(EntityType.Relation);

		// Action
		entityDao.save(relation);
	}

	@Test(expected = DaoException.class)
	public void saveEntity_withBound_shouldBeUnsupported() {
		// Setup
		Bound bound = mock(Bound.class);
		when(bound.getType()).thenReturn(EntityType.Bound);

		// Action
		entityDao.save(bound);
	}

	@Test
	public void saveAllEntities() throws Exception {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();
		Way way = OsmDataBuilder.buildSampleWay();
		List<Entity> entities = new ArrayList<Entity>();
		entities.add(node);
		entities.add(way);

		BulkRequestBuilder bulkRequestBuilderMocked = mock(BulkRequestBuilder.class);
		when(clientMocked.prepareBulk()).thenReturn(bulkRequestBuilderMocked);

		IndexRequestBuilder indexRequestBuilderMocked1 = mock(IndexRequestBuilder.class);
		doReturn(indexRequestBuilderMocked1).when(entityDao).buildIndexRequest(node);
		IndexRequestBuilder indexRequestBuilderMocked2 = mock(IndexRequestBuilder.class);
		doReturn(indexRequestBuilderMocked2).when(entityDao).buildIndexRequest(way);
		when(bulkRequestBuilderMocked.numberOfActions()).thenReturn(2);

		ListenableActionFuture<BulkResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(bulkRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		BulkResponse bulkResponseMocked = mock(BulkResponse.class);
		when(listenableActionFutureMocked.actionGet()).thenReturn(bulkResponseMocked);
		when(bulkResponseMocked.hasFailures()).thenReturn(false);

		// Action
		entityDao.saveAll(entities);

		// Assert
		verify(entityDao, times(1)).buildIndexRequest(node);
		verify(entityDao, times(1)).buildIndexRequest(way);
		verify(bulkRequestBuilderMocked, times(1)).add(indexRequestBuilderMocked1);
		verify(bulkRequestBuilderMocked, times(1)).add(indexRequestBuilderMocked2);
		verify(bulkRequestBuilderMocked, times(1)).execute();
		verify(listenableActionFutureMocked, times(1)).actionGet();

	}

	@Test
	public void saveAllEntities_withOneFailed() throws Exception {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();
		Way way = OsmDataBuilder.buildSampleWay();
		List<Entity> entities = new ArrayList<Entity>();
		entities.add(node);
		entities.add(way);

		BulkRequestBuilder bulkRequestBuilderMocked = mock(BulkRequestBuilder.class);
		when(clientMocked.prepareBulk()).thenReturn(bulkRequestBuilderMocked);

		IndexRequestBuilder indexRequestBuilderMocked1 = mock(IndexRequestBuilder.class);
		doReturn(indexRequestBuilderMocked1).when(entityDao).buildIndexRequest(node);
		doThrow(new RuntimeException("Simulated Exception")).when(entityDao).buildIndexRequest(way);
		when(bulkRequestBuilderMocked.numberOfActions()).thenReturn(1);

		ListenableActionFuture<BulkResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(bulkRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		BulkResponse bulkResponseMocked = mock(BulkResponse.class);
		when(listenableActionFutureMocked.actionGet()).thenReturn(bulkResponseMocked);
		when(bulkResponseMocked.hasFailures()).thenReturn(false);

		// Action
		entityDao.saveAll(entities);

		// Assert
		verify(entityDao, times(1)).buildIndexRequest(node);
		verify(entityDao, times(1)).buildIndexRequest(way);
		verify(bulkRequestBuilderMocked, times(1)).add(indexRequestBuilderMocked1);
		verify(bulkRequestBuilderMocked, times(1)).execute();
		verify(listenableActionFutureMocked, times(1)).actionGet();
	}

	@Test
	public void saveAllEntities_withAllFailed() throws Exception {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();
		Way way = OsmDataBuilder.buildSampleWay();
		List<Entity> entities = new ArrayList<Entity>();
		entities.add(node);
		entities.add(way);

		BulkRequestBuilder bulkRequestBuilderMocked = mock(BulkRequestBuilder.class);
		when(clientMocked.prepareBulk()).thenReturn(bulkRequestBuilderMocked);

		doThrow(new RuntimeException("Simulated Exception")).when(entityDao).buildIndexRequest(node);
		doThrow(new RuntimeException("Simulated Exception")).when(entityDao).buildIndexRequest(way);

		ListenableActionFuture<BulkResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(bulkRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		BulkResponse bulkResponseMocked = mock(BulkResponse.class);
		when(listenableActionFutureMocked.actionGet()).thenReturn(bulkResponseMocked);
		when(bulkResponseMocked.hasFailures()).thenReturn(false);

		// Action
		entityDao.saveAll(entities);

		// Assert
		verify(entityDao, times(1)).buildIndexRequest(node);
		verify(entityDao, times(1)).buildIndexRequest(way);
		verify(bulkRequestBuilderMocked, never()).execute();
	}

	@Test
	public void saveAllEntities_withNullList() throws Exception {
		// Action
		entityDao.saveAll(null);

		// Assert
		verifyNoMoreInteractions(clientMocked);
	}

	@Test
	public void saveAllEntities_withEmptyList() throws Exception {
		// Action
		entityDao.saveAll(new ArrayList<Entity>());

		// Assert
		verifyNoMoreInteractions(clientMocked);
	}

	@Test
	public void buildIndexRequest_withNode() throws IOException {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();

		IndexRequestBuilder indexRequestBuilderMocked = mock(IndexRequestBuilder.class);
		when(clientMocked.prepareIndex(any(String.class), any(String.class), any(String.class))).thenReturn(indexRequestBuilderMocked);
		when(indexRequestBuilderMocked.setSource(any(String.class))).thenReturn(indexRequestBuilderMocked);

		// Action
		IndexRequestBuilder actual = entityDao.buildIndexRequest(node);

		// Assert
		assertSame(indexRequestBuilderMocked, actual);
		verify(clientMocked).prepareIndex(INDEX_NAME, "node", "1");
		verify(indexRequestBuilderMocked).setSource("{\"shape\":{\"type\":\"point\",\"coordinates\":[2.0,1.0]},\"tags\":{\"highway\":\"traffic_signals\"}}");
	}

	@Test
	public void buildIndexRequest_withWay() throws IOException {
		// Setup
		Way way = OsmDataBuilder.buildSampleWay();

		ESNode esNode = OsmDataBuilder.buildSampleESNode();
		doReturn(Arrays.asList(new ESNode[] { esNode })).when(entityDao).findAll(ESNode.class, 1l);

		IndexRequestBuilder indexRequestBuilderMocked = mock(IndexRequestBuilder.class);
		when(clientMocked.prepareIndex(any(String.class), any(String.class), any(String.class))).thenReturn(indexRequestBuilderMocked);
		when(indexRequestBuilderMocked.setSource(any(String.class))).thenReturn(indexRequestBuilderMocked);

		// Action
		IndexRequestBuilder actual = entityDao.buildIndexRequest(way);

		// Assert
		assertSame(indexRequestBuilderMocked, actual);
		verify(clientMocked).prepareIndex(INDEX_NAME, "way", "1");
		verify(indexRequestBuilderMocked).setSource("{\"shape\":{\"type\":\"polygon\",\"coordinates\":[[[2.0,1.0]]]},\"tags\":{\"highway\":\"residential\"}}");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void buildIndexRequest_withRelation() throws IOException {
		// Setup
		Relation relation = mock(Relation.class);
		when(relation.getType()).thenReturn(EntityType.Relation);

		// Action
		entityDao.buildIndexRequest(relation);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void buildIndexRequest_withBound() throws IOException {
		// Setup
		Bound bound = mock(Bound.class);
		when(bound.getType()).thenReturn(EntityType.Bound);

		// Action
		entityDao.buildIndexRequest(bound);
	}

	/* FIND */

	@Test
	public void findEntity() throws Exception {
		// Setup
		ESNode node = mock(ESNode.class);

		Item item = mock(Item.class);
		doReturn(item).when(entityDao).buildGetItemRequest(ESEntityType.NODE, 1l);
		when(item.index()).thenReturn("index");
		when(item.type()).thenReturn(ESEntityType.NODE.getIndiceName());
		when(item.id()).thenReturn("1");
		String[] fields = new String[] { "field1", "field2" };
		when(item.fields()).thenReturn(fields);

		GetRequestBuilder getRequestBuilderMocked = mock(GetRequestBuilder.class);
		when(clientMocked.prepareGet(any(String.class), any(String.class), any(String.class))).thenReturn(getRequestBuilderMocked);
		when(getRequestBuilderMocked.setFields(fields)).thenReturn(getRequestBuilderMocked);
		ListenableActionFuture<GetResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(getRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		GetResponse getResponseMocked = mock(GetResponse.class);
		when(listenableActionFutureMocked.actionGet()).thenReturn(getResponseMocked);
		when(getResponseMocked.exists()).thenReturn(true);
		doReturn(node).when(entityDao).buildFromGetReponse(ESNode.class, getResponseMocked);

		// Action
		ESNode actual = entityDao.find(ESNode.class, 1);

		// Assert
		assertSame(node, actual);
		verify(entityDao, times(1)).buildGetItemRequest(ESEntityType.NODE, 1l);
		verify(clientMocked, times(1)).prepareGet("index", "node", "1");
	}

	@Test
	public void findEntity_withNotFound() throws Exception {
		// Setup

		Item item = mock(Item.class);
		doReturn(item).when(entityDao).buildGetItemRequest(ESEntityType.NODE, 1l);
		when(item.index()).thenReturn("index");
		when(item.type()).thenReturn("type");
		when(item.id()).thenReturn("1");
		String[] fields = new String[] { "field1", "field2" };
		when(item.fields()).thenReturn(fields);

		GetRequestBuilder getRequestBuilderMocked = mock(GetRequestBuilder.class);
		when(clientMocked.prepareGet(any(String.class), any(String.class), any(String.class))).thenReturn(getRequestBuilderMocked);
		when(getRequestBuilderMocked.setFields(fields)).thenReturn(getRequestBuilderMocked);
		ListenableActionFuture<GetResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(getRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		GetResponse getResponseMocked = mock(GetResponse.class);
		when(listenableActionFutureMocked.actionGet()).thenReturn(getResponseMocked);
		when(getResponseMocked.exists()).thenReturn(false);

		// Action
		ESNode actual = entityDao.find(ESNode.class, 1);

		// Assert
		assertNull(actual);
		verify(entityDao, times(1)).buildGetItemRequest(ESEntityType.NODE, 1l);
		verify(clientMocked, times(1)).prepareGet("index", "type", "1");
	}

	@Test(expected = DaoException.class)
	public void findEntity_withConnectorBroken() throws Exception {
		// Setup
		Item item = mock(Item.class);
		doReturn(item).when(entityDao).buildGetItemRequest(ESEntityType.NODE, 1l);
		when(item.index()).thenReturn("index");
		when(item.type()).thenReturn("type");
		when(item.id()).thenReturn("1");
		String[] fields = new String[] { "field1", "field2" };
		when(item.fields()).thenReturn(fields);

		GetRequestBuilder getRequestBuilderMocked = mock(GetRequestBuilder.class);
		when(clientMocked.prepareGet(any(String.class), any(String.class), any(String.class))).thenReturn(getRequestBuilderMocked);
		when(getRequestBuilderMocked.setFields(fields)).thenReturn(getRequestBuilderMocked);
		ListenableActionFuture<GetResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(getRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenThrow(new ElasticSearchException("Simulated exception"));

		// Action
		entityDao.find(ESNode.class, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void findEntity_withNullClass_shouldThrowException() {
		// Action
		entityDao.find(null, 1l);
	}

	@Test(expected = IllegalArgumentException.class)
	public void findEntity_withEntityClass_shouldThrowException() {
		// Action
		entityDao.find(ESEntity.class, 1l);
	}

	@Test
	public void buildGetItemRequest_withNode() {
		// Action
		Item nodeItem = entityDao.buildGetItemRequest(ESEntityType.NODE, 1l);

		// Assert
		assertEquals(INDEX_NAME, nodeItem.index());
		assertEquals(ESEntityType.NODE.getIndiceName(), nodeItem.type());
		assertEquals("1", nodeItem.id());
		assertTrue(Arrays.equals(new String[] { "shape", "tags" }, nodeItem.fields()));
	}

	@Test
	public void buildGetItemRequest_withWay() {
		// Action
		Item wayItem = entityDao.buildGetItemRequest(ESEntityType.WAY, 2l);

		// Assert
		assertEquals(INDEX_NAME, wayItem.index());
		assertEquals(ESEntityType.WAY.getIndiceName(), wayItem.type());
		assertEquals("2", wayItem.id());
		assertTrue(Arrays.equals(new String[] { "shape", "tags" }, wayItem.fields()));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void buildGetItemRequest_withRelation() {
		// Action
		entityDao.buildGetItemRequest(ESEntityType.RELATION, 1l);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void buildGetItemRequest_withBound() {
		// Action
		entityDao.buildGetItemRequest(ESEntityType.BOUND, 1l);
	}

	/* FIND ALL */

	@Test
	public void findAllEntities() {
		// Setup
		ESNode node1 = mock(ESNode.class);
		ESNode node2 = mock(ESNode.class);
		List<ESNode> expected = Arrays.asList(new ESNode[] { node1, node2 });

		MultiGetRequestBuilder multiGetRequestBuilderMocked = mock(MultiGetRequestBuilder.class);
		when(clientMocked.prepareMultiGet()).thenReturn(multiGetRequestBuilderMocked);
		when(multiGetRequestBuilderMocked.add(any(Item.class))).thenReturn(multiGetRequestBuilderMocked);

		Item item1 = mock(Item.class);
		when(entityDao.buildGetItemRequest(ESEntityType.NODE, 1l)).thenReturn(item1);
		Item item2 = mock(Item.class);
		when(entityDao.buildGetItemRequest(ESEntityType.NODE, 2l)).thenReturn(item2);

		ListenableActionFuture<MultiGetResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(multiGetRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		MultiGetResponse multiGetResponseMocked = mock(MultiGetResponse.class);
		when(listenableActionFutureMocked.actionGet()).thenReturn(multiGetResponseMocked);

		Iterator<MultiGetItemResponse> iterator = mock(Iterator.class);
		when(multiGetResponseMocked.iterator()).thenReturn(iterator);
		when(iterator.hasNext()).thenReturn(true, true, false);
		MultiGetItemResponse itemResponse1 = mock(MultiGetItemResponse.class);
		MultiGetItemResponse itemResponse2 = mock(MultiGetItemResponse.class);
		when(iterator.next()).thenReturn(itemResponse1, itemResponse2);

		GetResponse getResponseMocked1 = mock(GetResponse.class);
		when(getResponseMocked1.exists()).thenReturn(true);
		when(itemResponse1.getResponse()).thenReturn(getResponseMocked1);
		GetResponse getResponseMocked2 = mock(GetResponse.class);
		when(getResponseMocked2.exists()).thenReturn(true);
		when(itemResponse2.getResponse()).thenReturn(getResponseMocked2);

		doReturn(node1).when(entityDao).buildFromGetReponse(ESNode.class, getResponseMocked1);
		doReturn(node2).when(entityDao).buildFromGetReponse(ESNode.class, getResponseMocked2);

		// Action
		List<ESNode> actual = entityDao.findAll(ESNode.class, 1l, 2l);

		// Assert
		assertEquals(expected, actual);
		verify(entityDao, times(1)).buildGetItemRequest(ESEntityType.NODE, 1l);
		verify(entityDao, times(1)).buildGetItemRequest(ESEntityType.NODE, 2l);
		verify(clientMocked, times(1)).prepareMultiGet();
	}

	@Test(expected = DaoException.class)
	public void findAllEntities_withNotFound() {
		// Setup
		MultiGetRequestBuilder multiGetRequestBuilderMocked = mock(MultiGetRequestBuilder.class);
		when(clientMocked.prepareMultiGet()).thenReturn(multiGetRequestBuilderMocked);
		when(multiGetRequestBuilderMocked.add(any(Item.class))).thenReturn(multiGetRequestBuilderMocked);

		Item item1 = mock(Item.class);
		when(entityDao.buildGetItemRequest(ESEntityType.NODE, 1l)).thenReturn(item1);
		Item item2 = mock(Item.class);
		when(entityDao.buildGetItemRequest(ESEntityType.NODE, 2l)).thenReturn(item2);

		ListenableActionFuture<MultiGetResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(multiGetRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		MultiGetResponse multiGetResponseMocked = mock(MultiGetResponse.class);
		when(listenableActionFutureMocked.actionGet()).thenReturn(multiGetResponseMocked);

		Iterator<MultiGetItemResponse> iterator = mock(Iterator.class);
		when(multiGetResponseMocked.iterator()).thenReturn(iterator);
		when(iterator.hasNext()).thenReturn(true, true, false);
		MultiGetItemResponse itemResponse1 = mock(MultiGetItemResponse.class);
		MultiGetItemResponse itemResponse2 = mock(MultiGetItemResponse.class);
		when(iterator.next()).thenReturn(itemResponse1, itemResponse2);

		GetResponse getResponseMocked1 = mock(GetResponse.class);
		when(getResponseMocked1.exists()).thenReturn(true);
		when(itemResponse1.getResponse()).thenReturn(getResponseMocked1);
		GetResponse getResponseMocked2 = mock(GetResponse.class);
		when(getResponseMocked2.exists()).thenReturn(false);
		when(itemResponse2.getResponse()).thenReturn(getResponseMocked2);

		doReturn(mock(ESNode.class)).when(entityDao).buildFromGetReponse(ESNode.class, getResponseMocked1);

		// Action
		entityDao.findAll(ESNode.class, 1l, 2l);
	}

	@Test(expected = DaoException.class)
	public void findAllEntities_withConnectorBroken() {
		// Setup
		MultiGetRequestBuilder multiGetRequestBuilderMocked = mock(MultiGetRequestBuilder.class);
		when(clientMocked.prepareMultiGet()).thenReturn(multiGetRequestBuilderMocked);
		when(multiGetRequestBuilderMocked.add(any(Item.class))).thenReturn(multiGetRequestBuilderMocked);

		Item item1 = mock(Item.class);
		when(entityDao.buildGetItemRequest(ESEntityType.NODE, 1l)).thenReturn(item1);
		Item item2 = mock(Item.class);
		when(entityDao.buildGetItemRequest(ESEntityType.NODE, 2l)).thenReturn(item2);

		ListenableActionFuture<MultiGetResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(multiGetRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenThrow(new ElasticSearchException("Simulated exception"));

		// Action
		entityDao.findAll(ESNode.class, 1l, 2l);
	}

	/* DELETE */

	@Test
	public void delete() {
		// Setup
		DeleteRequestBuilder deleteRequestBuilder = mock(DeleteRequestBuilder.class);
		when(clientMocked.prepareDelete(any(String.class), any(String.class), any(String.class))).thenReturn(deleteRequestBuilder);
		ListenableActionFuture<DeleteResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(deleteRequestBuilder.execute()).thenReturn(listenableActionFutureMocked);
		DeleteResponse deleteResponseMocked = mock(DeleteResponse.class);
		when(listenableActionFutureMocked.actionGet()).thenReturn(deleteResponseMocked);
		when(deleteResponseMocked.notFound()).thenReturn(false);

		// Action
		boolean actual = entityDao.delete(ESNode.class, 1l);

		// Assert
		verify(clientMocked).prepareDelete(INDEX_NAME, "node", "1");
		assertTrue(actual);
	}

	@Test
	public void delete_withNotFoundDocument() {
		// Setup
		DeleteRequestBuilder deleteRequestBuilder = mock(DeleteRequestBuilder.class);
		when(clientMocked.prepareDelete(any(String.class), any(String.class), any(String.class))).thenReturn(deleteRequestBuilder);
		ListenableActionFuture<DeleteResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(deleteRequestBuilder.execute()).thenReturn(listenableActionFutureMocked);
		DeleteResponse deleteResponseMocked = mock(DeleteResponse.class);
		when(listenableActionFutureMocked.actionGet()).thenReturn(deleteResponseMocked);
		when(deleteResponseMocked.notFound()).thenReturn(true);

		// Action
		boolean actual = entityDao.delete(ESNode.class, 1l);

		// Assert
		assertFalse(actual);
	}

	@Test(expected = DaoException.class)
	public void delete_withBrokenConnector() {
		// Setup
		DeleteRequestBuilder deleteRequestBuilder = mock(DeleteRequestBuilder.class);
		when(clientMocked.prepareDelete(any(String.class), any(String.class), any(String.class))).thenReturn(deleteRequestBuilder);
		ListenableActionFuture<DeleteResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(deleteRequestBuilder.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenThrow(new ElasticSearchException("Simulated exception"));

		// Action
		entityDao.delete(ESNode.class, 1l);
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

	public class ItemMatcher extends BaseMatcher<Item> {

		private final Item expected;

		public ItemMatcher(Item expected) {
			this.expected = expected;
		}

		@Override
		public boolean matches(Object item) {
			if (expected == item) return true;
			if (item == null) return false;
			if (expected.getClass() != item.getClass()) return false;
			Item other = (Item) item;
			return expected.index().equals(other.index())
					&& expected.type().equals(other.type())
					&& expected.id().equals(other.id())
					&& Arrays.equals(expected.fields(), other.fields());
		}

		@Override
		public void describeTo(Description description) {}

	}

}
