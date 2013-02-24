package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
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

	/* save */

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

	@Test(expected = DaoException.class)
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
		doThrow(new IOException("Simulated Exception")).when(entityDao).buildIndexRequest(way);

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
	public void saveNode() throws Exception {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();
		XContentBuilder xContentBuilder = new EntityMapper().marshallNode(node);

		IndexRequestBuilder indexRequestBuilderMocked = mock(IndexRequestBuilder.class);
		when(clientMocked.prepareIndex(any(String.class), any(String.class), any(String.class))).thenReturn(indexRequestBuilderMocked);
		when(entityMapperMocked.marshallNode(node)).thenReturn(xContentBuilder);
		when(indexRequestBuilderMocked.setSource(any(XContentBuilder.class))).thenReturn(indexRequestBuilderMocked);

		// Action
		IndexRequestBuilder actual = entityDao.buildIndexRequest(node);

		// Assert
		assertSame(indexRequestBuilderMocked, actual);
		verify(clientMocked).prepareIndex(INDEX_NAME, "node", "1");
		verify(entityMapperMocked).marshallNode(node);
		verify(indexRequestBuilderMocked).setSource(xContentBuilder);
	}

	@Test
	public void saveWay() throws Exception {
		// Setup
		Way way = OsmDataBuilder.buildSampleWay();
		XContentBuilder xContentBuilder = new EntityMapper().marshallWay(way);

		IndexRequestBuilder indexRequestBuilderMocked = mock(IndexRequestBuilder.class);
		when(clientMocked.prepareIndex(any(String.class), any(String.class), any(String.class))).thenReturn(indexRequestBuilderMocked);
		when(entityMapperMocked.marshallWay(way)).thenReturn(xContentBuilder);
		when(indexRequestBuilderMocked.setSource(any(XContentBuilder.class))).thenReturn(indexRequestBuilderMocked);

		// Action
		IndexRequestBuilder actual = entityDao.buildIndexRequest(way);

		// Assert
		assertSame(indexRequestBuilderMocked, actual);
		verify(clientMocked).prepareIndex(INDEX_NAME, "way", "1");
		verify(entityMapperMocked).marshallWay(way);
		verify(indexRequestBuilderMocked).setSource(xContentBuilder);
	}

	/* find */

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

	/* findAll */

	@Test
	public void findAllEntities() {
		// Setup
		List<Node> nodes = Arrays.asList(new Node[] { mock(Node.class) });
		List<Way> ways = Arrays.asList(new Way[] { mock(Way.class) });
		List<Relation> relations = Arrays.asList(new Relation[] { mock(Relation.class) });
		List<Bound> bounds = Arrays.asList(new Bound[] { mock(Bound.class) });

		doReturn(nodes).when(entityDao).findAllNodes(10l, 11l);
		doReturn(ways).when(entityDao).findAllWays(20l, 21l);
		doReturn(relations).when(entityDao).findAllRelations(30l, 31l);
		doReturn(bounds).when(entityDao).findAllBounds(40l, 41l);

		// Action
		List<Node> resultNodes = entityDao.findAll(Node.class, 10l, 11l);
		List<Way> resultWays = entityDao.findAll(Way.class, 20l, 21l);
		List<Relation> resultRelations = entityDao.findAll(Relation.class, 30l, 31l);
		List<Bound> resultBounds = entityDao.findAll(Bound.class, 40l, 41l);

		// Assert
		verify(entityDao, times(1)).findAllNodes(10l, 11l);
		verify(entityDao, times(1)).findAllWays(20l, 21l);
		verify(entityDao, times(1)).findAllRelations(30l, 31l);
		verify(entityDao, times(1)).findAllBounds(40l, 41l);

		assertSame(nodes, resultNodes);
		assertSame(ways, resultWays);
		assertSame(relations, resultRelations);
		assertSame(bounds, resultBounds);
	}

	@Test(expected = IllegalArgumentException.class)
	public void findAllEntities_withNullClass_shouldThrowException() {
		// Action
		entityDao.findAll(null, new long[] { 1l });
	}

	@Test(expected = IllegalArgumentException.class)
	public void findAllEntities_withEntityClass_shouldThrowException() {
		// Action
		entityDao.findAll(Entity.class, new long[] { 1l });
	}

	@Test(expected = IllegalArgumentException.class)
	public void findAllEntities_withNullArray_shouldThrowException() {
		// Action
		entityDao.findAll(Node.class, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void findAllEntities_withEmptyArray_shouldThrowException() {
		// Action
		entityDao.findAll(Node.class, new long[0]);
	}

	@Test
	public void findAllNodes() {
		// Setup
		Node expected1 = OsmDataBuilder.buildSampleNode();
		expected1.setId(1l);
		Node expected2 = OsmDataBuilder.buildSampleNode();
		expected2.setId(2l);

		MultiGetRequestBuilder multiGetRequestBuilderMocked = mock(MultiGetRequestBuilder.class);
		ListenableActionFuture<MultiGetResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		MultiGetResponse multiGetResponseMocked = mock(MultiGetResponse.class);
		MultiGetItemResponse multiGetItemResponseMocked = mock(MultiGetItemResponse.class);
		GetResponse getResponseMocked = mock(GetResponse.class);

		when(clientMocked.prepareMultiGet()).thenReturn(multiGetRequestBuilderMocked);
		when(multiGetRequestBuilderMocked.add(any(Item.class))).thenReturn(multiGetRequestBuilderMocked);
		when(multiGetRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenReturn(multiGetResponseMocked);
		when(multiGetResponseMocked.responses()).thenReturn(new MultiGetItemResponse[] { multiGetItemResponseMocked, multiGetItemResponseMocked });
		when(multiGetItemResponseMocked.failed()).thenReturn(false);
		when(multiGetItemResponseMocked.response()).thenReturn(getResponseMocked);
		when(entityMapperMocked.unmarshallNode(any(GetResponse.class))).thenReturn(expected1).thenReturn(expected2);

		// Action
		List<Node> actual = entityDao.findAllNodes(1l, 2l);

		// Assert
		verify(multiGetRequestBuilderMocked).add(argThat(new ItemMatcher(new Item(INDEX_NAME, "node", "1").fields("location", "tags"))));
		verify(multiGetRequestBuilderMocked).add(argThat(new ItemMatcher(new Item(INDEX_NAME, "node", "2").fields("location", "tags"))));
		assertEquals(Arrays.asList(new Node[] { expected1, expected2 }), actual);
	}

	@Test(expected = DaoException.class)
	public void findAllNodes_withFailedItem_shouldThrowDaoException() {
		// Setup
		Node expected1 = OsmDataBuilder.buildSampleNode();
		expected1.setId(1l);

		MultiGetRequestBuilder multiGetRequestBuilderMocked = mock(MultiGetRequestBuilder.class);
		ListenableActionFuture<MultiGetResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		MultiGetResponse multiGetResponseMocked = mock(MultiGetResponse.class);
		MultiGetItemResponse multiGetItemResponseMocked = mock(MultiGetItemResponse.class);
		GetResponse getResponseMocked = mock(GetResponse.class);

		when(clientMocked.prepareMultiGet()).thenReturn(multiGetRequestBuilderMocked);
		when(multiGetRequestBuilderMocked.add(any(Item.class))).thenReturn(multiGetRequestBuilderMocked);
		when(multiGetRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenReturn(multiGetResponseMocked);
		when(multiGetResponseMocked.responses()).thenReturn(new MultiGetItemResponse[] { multiGetItemResponseMocked, multiGetItemResponseMocked });
		when(multiGetItemResponseMocked.failed()).thenReturn(false).thenReturn(true);
		when(multiGetItemResponseMocked.response()).thenReturn(getResponseMocked).thenReturn(null);
		when(entityMapperMocked.unmarshallNode(any(GetResponse.class))).thenReturn(expected1);

		// Action
		entityDao.findAllNodes(1l, 2l);
	}

	@Test(expected = DaoException.class)
	public void findAllNodes_withClientException_shouldThrowDaoException() throws IOException {
		// Setup
		when(clientMocked.prepareSearch(any(String.class))).thenThrow(new ElasticSearchException("Simulated Exception"));

		// Action
		entityDao.findAllNodes(1l);
	}

	@Test
	public void findAllWays() {
		// Setup
		Way expected1 = OsmDataBuilder.buildSampleWay();
		expected1.setId(1l);
		Way expected2 = OsmDataBuilder.buildSampleWay();
		expected2.setId(2l);

		MultiGetRequestBuilder multiGetRequestBuilderMocked = mock(MultiGetRequestBuilder.class);
		ListenableActionFuture<MultiGetResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		MultiGetResponse multiGetResponseMocked = mock(MultiGetResponse.class);
		MultiGetItemResponse multiGetItemResponseMocked = mock(MultiGetItemResponse.class);
		GetResponse getResponseMocked = mock(GetResponse.class);

		when(clientMocked.prepareMultiGet()).thenReturn(multiGetRequestBuilderMocked);
		when(multiGetRequestBuilderMocked.add(any(Item.class))).thenReturn(multiGetRequestBuilderMocked);
		when(multiGetRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenReturn(multiGetResponseMocked);
		when(multiGetResponseMocked.responses()).thenReturn(new MultiGetItemResponse[] { multiGetItemResponseMocked, multiGetItemResponseMocked });
		when(multiGetItemResponseMocked.failed()).thenReturn(false);
		when(multiGetItemResponseMocked.response()).thenReturn(getResponseMocked);
		when(entityMapperMocked.unmarshallWay(any(GetResponse.class))).thenReturn(expected1).thenReturn(expected2);

		// Action
		List<Way> actual = entityDao.findAllWays(1l, 2l);

		// Assert
		verify(multiGetRequestBuilderMocked).add(argThat(new ItemMatcher(new Item(INDEX_NAME, "way", "1").fields("tags", "nodes"))));
		verify(multiGetRequestBuilderMocked).add(argThat(new ItemMatcher(new Item(INDEX_NAME, "way", "2").fields("tags", "nodes"))));
		assertEquals(Arrays.asList(new Way[] { expected1, expected2 }), actual);
	}

	@Test(expected = DaoException.class)
	public void findAllWays_withFailedItem_shouldThrowDaoException() {
		// Setup
		Way expected1 = OsmDataBuilder.buildSampleWay();
		expected1.setId(1l);

		MultiGetRequestBuilder multiGetRequestBuilderMocked = mock(MultiGetRequestBuilder.class);
		ListenableActionFuture<MultiGetResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		MultiGetResponse multiGetResponseMocked = mock(MultiGetResponse.class);
		MultiGetItemResponse multiGetItemResponseMocked = mock(MultiGetItemResponse.class);
		GetResponse getResponseMocked = mock(GetResponse.class);

		when(clientMocked.prepareMultiGet()).thenReturn(multiGetRequestBuilderMocked);
		when(multiGetRequestBuilderMocked.add(any(Item.class))).thenReturn(multiGetRequestBuilderMocked);
		when(multiGetRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		when(listenableActionFutureMocked.actionGet()).thenReturn(multiGetResponseMocked);
		when(multiGetResponseMocked.responses()).thenReturn(new MultiGetItemResponse[] { multiGetItemResponseMocked, multiGetItemResponseMocked });
		when(multiGetItemResponseMocked.failed()).thenReturn(false).thenReturn(true);
		when(multiGetItemResponseMocked.response()).thenReturn(getResponseMocked).thenReturn(null);
		when(entityMapperMocked.unmarshallWay(any(GetResponse.class))).thenReturn(expected1);

		// Action
		entityDao.findAllWays(1l, 2l);
	}

	@Test(expected = DaoException.class)
	public void findAllWays_withClientException_shouldThrowDaoException() throws IOException {
		// Setup
		when(clientMocked.prepareSearch(any(String.class))).thenThrow(new ElasticSearchException("Simulated Exception"));

		// Action
		entityDao.findAllWays(1l);
	}

	/* delete */

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
