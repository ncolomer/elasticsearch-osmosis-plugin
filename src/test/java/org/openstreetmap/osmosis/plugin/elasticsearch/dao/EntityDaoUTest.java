package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import java.util.*;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
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
import org.elasticsearch.client.Client;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESEntity;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESEntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESShape;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESShape.ESShapeBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.OsmDataBuilder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

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

		doNothing().when(entityDao).saveAll(any(List.class));

		// Action
		entityDao.save(entity);

		// Assert
		verify(entityDao, times(1)).saveAll(eq(Arrays.asList(entity)));
	}

	@Test(expected = IllegalArgumentException.class)
	public void saveEntity_withNull_shouldThrowException() {
		// Action
		entityDao.save(null);
	}

	/* SAVE ALL */

	@Test
	public void saveAll() {
		// Setup
		Node node = mock(Node.class);
		Way way = mock(Way.class);
		Relation relation = mock(Relation.class);
		Bound bound = mock(Bound.class);

		when(node.getType()).thenReturn(EntityType.Node);
		when(way.getType()).thenReturn(EntityType.Way);
		when(relation.getType()).thenReturn(EntityType.Relation);
		when(bound.getType()).thenReturn(EntityType.Bound);

		doNothing().when(entityDao).saveAllNodes(any(List.class));
		doNothing().when(entityDao).saveAllWays(any(List.class));

		// Action
		entityDao.saveAll(Arrays.asList(node, way, relation, bound));

		// Assert
		verify(entityDao, times(1)).saveAllNodes(eq(Arrays.asList(node)));
		verify(entityDao, times(1)).saveAllWays(eq(Arrays.asList(way)));
	}

	@Test
	public void saveAll_withNullList() throws Exception {
		// Action
		entityDao.saveAll(null);

		// Assert
		verifyNoMoreInteractions(clientMocked);
	}

	@Test
	public void saveAll_withEmptyList() throws Exception {
		// Action
		entityDao.saveAll(new ArrayList<Entity>());

		// Assert
		verifyNoMoreInteractions(clientMocked);
	}

	@Test
	public void saveAllNodes() {
		// Setup
		Node node = OsmDataBuilder.buildSampleNode();

		Iterator<MultiGetItemResponse> iteratorMocked = mock(Iterator.class);
		doReturn(iteratorMocked).when(entityDao).getNodeItems(any(List.class));

		ESShape builder = new ESShapeBuilder(1).addLocation(1.0, 2.0).build();
		doReturn(builder).when(entityDao).getShape(iteratorMocked, 1);

		BulkRequestBuilder bulkRequestBuilderMocked = mock(BulkRequestBuilder.class);
		when(clientMocked.prepareBulk()).thenReturn(bulkRequestBuilderMocked);

		IndexRequestBuilder indexRequestBuilderMocked = mock(IndexRequestBuilder.class);
		when(indexRequestBuilderMocked.setSource(any(String.class))).thenReturn(indexRequestBuilderMocked);
		when(clientMocked.prepareIndex(any(String.class), any(String.class), any(String.class)))
				.thenReturn(indexRequestBuilderMocked);

		// Action
		entityDao.saveAllNodes(Arrays.asList(node));

		// Assert
		String source = "{\"centroid\":[2.0,1.0],\"shape\":{\"type\":\"point\",\"coordinates\":[2.0,1.0]},\"tags\":{\"highway\":\"traffic_signals\"}}";
		verify(clientMocked).prepareIndex(INDEX_NAME, ESEntityType.NODE.getIndiceName(), "1");
		verify(indexRequestBuilderMocked).setSource(source);
		verify(bulkRequestBuilderMocked).add(indexRequestBuilderMocked);
		verify(entityDao).executeBulkRequest(bulkRequestBuilderMocked);
	}

	@Test
	public void saveAllWays() {
		// Setup
		Way way = OsmDataBuilder.buildSampleWay(1, 1, 2, 3, 4);

		Iterator<MultiGetItemResponse> iteratorMocked = mock(Iterator.class);
		doReturn(iteratorMocked).when(entityDao).getNodeItems(any(List.class));

		ESShape builder = new ESShapeBuilder(1).addLocation(1.0, 2.0).addLocation(2.0, 3.0)
				.addLocation(3.0, 2.0).addLocation(1.0, 2.0).build();
		doReturn(builder).when(entityDao).getShape(iteratorMocked, 4);

		BulkRequestBuilder bulkRequestBuilderMocked = mock(BulkRequestBuilder.class);
		when(clientMocked.prepareBulk()).thenReturn(bulkRequestBuilderMocked);

		IndexRequestBuilder indexRequestBuilderMocked = mock(IndexRequestBuilder.class);
		when(indexRequestBuilderMocked.setSource(any(String.class))).thenReturn(indexRequestBuilderMocked);
		when(clientMocked.prepareIndex(any(String.class), any(String.class), any(String.class)))
				.thenReturn(indexRequestBuilderMocked);

		// Action
		List<Way> ways = Arrays.asList(way);
		entityDao.saveAllWays(ways);

		// Assert
		verify(entityDao).getNodeItems(ways);
		String source = "{\"centroid\":[2.3333333333333335,2.0],\"lengthKm\":536.8973391277414," +
				"\"areaKm2\":12364.345757132623,\"shape\":{\"type\":\"polygon\",\"coordinates\":" +
				"[[[2.0,1.0],[3.0,2.0],[2.0,3.0],[2.0,1.0]]]},\"tags\":{\"highway\":\"residential\"}}";
		verify(clientMocked).prepareIndex(INDEX_NAME, ESEntityType.WAY.getIndiceName(), "1");
		verify(indexRequestBuilderMocked).setSource(source);
		verify(bulkRequestBuilderMocked).add(indexRequestBuilderMocked);
		verify(entityDao).executeBulkRequest(bulkRequestBuilderMocked);
	}

	@Test
	public void getNodeItems() {
		// Setup
		Way way = OsmDataBuilder.buildSampleWay();

		MultiGetRequestBuilder multiGetRequestBuilderMocked = mock(MultiGetRequestBuilder.class);
		when(clientMocked.prepareMultiGet()).thenReturn(multiGetRequestBuilderMocked);

		ListenableActionFuture<MultiGetResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(multiGetRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		MultiGetResponse multiGetResponseMocked = mock(MultiGetResponse.class);
		when(listenableActionFutureMocked.actionGet()).thenReturn(multiGetResponseMocked);
		Iterator<MultiGetItemResponse> iteratorMocked = mock(Iterator.class);
		when(multiGetResponseMocked.iterator()).thenReturn(iteratorMocked);

		// Action
		Iterator<MultiGetItemResponse> actual = entityDao.getNodeItems(Arrays.asList(way));

		// Assert
		Item item = new Item(INDEX_NAME, ESEntityType.NODE.getIndiceName(), "1");
		verify(multiGetRequestBuilderMocked).add(argThat(new ItemMatcher(item)));
		verify(multiGetRequestBuilderMocked, times(1)).execute();
		Assert.assertSame(iteratorMocked, actual);
	}

	@Test
	public void getLocationArrayBuilder() {
		// Setup
		GetResponse response1 = mock(GetResponse.class, Mockito.RETURNS_DEEP_STUBS);
		when(response1.isExists()).thenReturn(true);
		Map<String, Object> map1 = mock(Map.class);
		when(response1.getSource().get("shape")).thenReturn(map1);
		when(map1.get("coordinates")).thenReturn(Arrays.asList(2.0, 1.0));

		GetResponse response2 = mock(GetResponse.class, Mockito.RETURNS_DEEP_STUBS);
		when(response2.isExists()).thenReturn(true);
		Map<String, Object> map2 = mock(Map.class);
		when(response2.getSource().get("shape")).thenReturn(map2);
		when(map2.get("coordinates")).thenReturn(Arrays.asList(4.0, 3.0));

		MultiGetItemResponse multiGetItemResponseMocked = mock(MultiGetItemResponse.class);
		when(multiGetItemResponseMocked.getResponse()).thenReturn(response1, response2);

		Iterator<MultiGetItemResponse> iteratorMocked = mock(Iterator.class);
		when(iteratorMocked.next()).thenReturn(multiGetItemResponseMocked);

		// Action
		ESShape actual = entityDao.getShape(iteratorMocked, 2);

		// Assert
		Assert.assertTrue(Arrays.deepEquals(new double[][] {
				new double[] { 2.0, 1.0 },
				new double[] { 4.0, 3.0 }
		}, actual.getGeoJsonArray()));
	}

	@Test
	public void getLocationArrayBuilder_withNotExistingResponse() {
		// Setup
		GetResponse response1 = mock(GetResponse.class, Mockito.RETURNS_DEEP_STUBS);
		when(response1.isExists()).thenReturn(true);
		Map<String, Object> map1 = mock(Map.class);
		when(response1.getSource().get("shape")).thenReturn(map1);
		when(map1.get("coordinates")).thenReturn(Arrays.asList(2.0, 1.0));

		GetResponse response2 = mock(GetResponse.class, Mockito.RETURNS_DEEP_STUBS);
		when(response2.isExists()).thenReturn(false);

		MultiGetItemResponse multiGetItemResponseMocked = mock(MultiGetItemResponse.class);
		when(multiGetItemResponseMocked.getResponse()).thenReturn(response1, response2);

		Iterator<MultiGetItemResponse> iteratorMocked = mock(Iterator.class);
		when(iteratorMocked.next()).thenReturn(multiGetItemResponseMocked);

		// Action
		ESShape actual = entityDao.getShape(iteratorMocked, 2);

		// Assert
		Assert.assertTrue(Arrays.deepEquals(new double[][] {
				new double[] { 2.0, 1.0 }
		}, actual.getGeoJsonArray()));
	}

	@Test
	public void executeBulkRequest() {
		// Setup
		BulkRequestBuilder bulkRequestBuilderMocked = mock(BulkRequestBuilder.class);
		when(bulkRequestBuilderMocked.numberOfActions()).thenReturn(1);

		ListenableActionFuture<BulkResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(bulkRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		BulkResponse bulkResponseMocked = mock(BulkResponse.class);
		when(listenableActionFutureMocked.actionGet()).thenReturn(bulkResponseMocked);
		when(bulkResponseMocked.hasFailures()).thenReturn(false);

		// Action
		entityDao.executeBulkRequest(bulkRequestBuilderMocked);

		// Assert
		verify(bulkRequestBuilderMocked, times(1)).execute();
		verify(bulkResponseMocked, times(0)).iterator();
	}

	@Test
	public void executeBulkRequest_withFailure() {
		// Setup
		BulkRequestBuilder bulkRequestBuilderMocked = mock(BulkRequestBuilder.class);
		when(bulkRequestBuilderMocked.numberOfActions()).thenReturn(1);

		ListenableActionFuture<BulkResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(bulkRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		BulkResponse bulkResponseMocked = mock(BulkResponse.class);
		when(listenableActionFutureMocked.actionGet()).thenReturn(bulkResponseMocked);
		when(bulkResponseMocked.hasFailures()).thenReturn(true);

		Iterator<BulkItemResponse> iteratorMocked = mock(Iterator.class);
		when(bulkResponseMocked.iterator()).thenReturn(iteratorMocked);
		BulkItemResponse response1 = mock(BulkItemResponse.class);
		when(response1.isFailed()).thenReturn(true);
		when(iteratorMocked.hasNext()).thenReturn(true, false);
		when(iteratorMocked.next()).thenReturn(response1);

		// Action
		entityDao.executeBulkRequest(bulkRequestBuilderMocked);

		// Assert
		verify(bulkRequestBuilderMocked, times(1)).execute();
		verify(bulkResponseMocked, times(1)).iterator();
	}

	@Test
	public void executeBulkRequest_withNoResult() {
		// Setup
		BulkRequestBuilder bulkRequestBuilderMocked = mock(BulkRequestBuilder.class);
		when(bulkRequestBuilderMocked.numberOfActions()).thenReturn(0);

		// Action
		entityDao.executeBulkRequest(bulkRequestBuilderMocked);

		// Assert
		verify(bulkRequestBuilderMocked, times(0)).execute();
	}

	/* FIND */

	@Test
	public void find() throws Exception {
		// Setup
		ESNode node = mock(ESNode.class);

		doReturn(Arrays.asList(node)).when(entityDao).findAll(ESNode.class, 1);

		// Action
		entityDao.find(ESNode.class, 1);

		// Assert
		verify(entityDao, times(1)).findAll(ESNode.class, 1);
	}

	/* FIND ALL */

	@Test
	public void findAll() {
		// Setup
		Node node1 = OsmDataBuilder.buildSampleNode(1);
		Node node2 = OsmDataBuilder.buildSampleNode(2);

		MultiGetRequestBuilder multiGetRequestBuilderMocked = mock(MultiGetRequestBuilder.class);
		doReturn(multiGetRequestBuilderMocked).when(entityDao).buildMultiGetRequest(ESNode.class, 1, 2);
		doReturn(Arrays.asList(node1, node2)).when(entityDao).executeMultiGetRequest(ESNode.class, multiGetRequestBuilderMocked);

		// Action
		List<ESNode> nodes = entityDao.findAll(ESNode.class, 1, 2);

		// Assert
		verify(entityDao).buildMultiGetRequest(ESNode.class, 1, 2);
		verify(entityDao).executeMultiGetRequest(ESNode.class, multiGetRequestBuilderMocked);
		Assert.assertEquals(Arrays.asList(node1, node2), nodes);
	}

	@Test
	public void findAll_withEmptyArray() {
		// Action
		entityDao.findAll(ESNode.class);

		// Assert
		verify(entityDao, times(0)).buildMultiGetRequest(any(Class.class), any(long[].class));
		verify(entityDao, times(0)).executeMultiGetRequest(any(Class.class), any(MultiGetRequestBuilder.class));
	}

	@Test
	public void findAll_withNullArray() {
		// Action
		entityDao.findAll(ESNode.class, null);

		// Assert
		verify(entityDao, times(0)).buildMultiGetRequest(any(Class.class), any(long[].class));
		verify(entityDao, times(0)).executeMultiGetRequest(any(Class.class), any(MultiGetRequestBuilder.class));
	}

	@Test(expected = DaoException.class)
	public void findAll_withException() {
		// Setup
		MultiGetRequestBuilder multiGetRequestBuilderMocked = mock(MultiGetRequestBuilder.class);
		doReturn(multiGetRequestBuilderMocked).when(entityDao).buildMultiGetRequest(ESNode.class, 1, 2);
		doThrow(new RuntimeException("Simulated Exception")).when(entityDao).executeMultiGetRequest(ESNode.class, multiGetRequestBuilderMocked);

		// Action
		entityDao.findAll(ESNode.class, 1, 2);
	}

	@Test
	public void buildMultiGetRequest() {
		// Setup
		MultiGetRequestBuilder multiGetRequestBuilderMocked = mock(MultiGetRequestBuilder.class);
		when(clientMocked.prepareMultiGet()).thenReturn(multiGetRequestBuilderMocked);

		// Action
		MultiGetRequestBuilder actual = entityDao.buildMultiGetRequest(ESNode.class, 1, 2);

		// Assert
		Item item1 = new Item(INDEX_NAME, ESEntityType.NODE.getIndiceName(), "1");
		Item item2 = new Item(INDEX_NAME, ESEntityType.NODE.getIndiceName(), "2");
		verify(multiGetRequestBuilderMocked).add(argThat(new ItemMatcher(item1)));
		verify(multiGetRequestBuilderMocked).add(argThat(new ItemMatcher(item2)));
		Assert.assertSame(multiGetRequestBuilderMocked, actual);
	}

	@Test(expected = IllegalArgumentException.class)
	public void buildMultiGetRequest_withInvalidClass() {
		// Action
		entityDao.buildMultiGetRequest(ESEntity.class, 1);
	}

	@Test
	public void executeMultiGetRequest() {
		// Setup
		ESNode node1 = OsmDataBuilder.buildSampleESNode(1);
		ESNode node2 = OsmDataBuilder.buildSampleESNode(2);

		MultiGetRequestBuilder multiGetRequestBuilderMocked = mock(MultiGetRequestBuilder.class);

		ListenableActionFuture<MultiGetResponse> listenableActionFutureMocked = mock(ListenableActionFuture.class);
		when(multiGetRequestBuilderMocked.execute()).thenReturn(listenableActionFutureMocked);
		MultiGetResponse multiGetResponseMocked = mock(MultiGetResponse.class);
		when(listenableActionFutureMocked.actionGet()).thenReturn(multiGetResponseMocked);

		Iterator<MultiGetItemResponse> iterator = mock(Iterator.class);
		when(multiGetResponseMocked.iterator()).thenReturn(iterator);
		MultiGetItemResponse item1 = mock(MultiGetItemResponse.class);
		MultiGetItemResponse item2 = mock(MultiGetItemResponse.class);
		when(iterator.hasNext()).thenReturn(true, true, false);
		when(iterator.next()).thenReturn(item1, item2);

		doReturn(node1).when(entityDao).buildEntityFromGetResponse(ESNode.class, item1);
		doReturn(node2).when(entityDao).buildEntityFromGetResponse(ESNode.class, item2);

		// Action
		List<ESNode> actual = entityDao.executeMultiGetRequest(ESNode.class, multiGetRequestBuilderMocked);

		// Assert
		Assert.assertEquals(Arrays.asList(node1, node2), actual);
	}

	@Test(expected = DaoException.class)
	public void buildEntityFromGetResponse_withNotExistingResponse() {
		// Setup
		MultiGetItemResponse multiGetItemResponseMocked = mock(MultiGetItemResponse.class);
		GetResponse getResponseMocked = mock(GetResponse.class);
		when(getResponseMocked.isExists()).thenReturn(false);
		when(multiGetItemResponseMocked.getResponse()).thenReturn(getResponseMocked);

		// Action
		entityDao.buildEntityFromGetResponse(ESNode.class, multiGetItemResponseMocked);
	}

	@Test(expected = IllegalArgumentException.class)
	public void buildEntityFromGetResponse_withNullType() {
		// Setup
		MultiGetItemResponse multiGetItemResponseMocked = mock(MultiGetItemResponse.class);
		GetResponse getResponseMocked = mock(GetResponse.class);
		when(getResponseMocked.isExists()).thenReturn(true);
		when(multiGetItemResponseMocked.getResponse()).thenReturn(getResponseMocked);

		// Action
		entityDao.buildEntityFromGetResponse(null, multiGetItemResponseMocked);
	}

	@Test(expected = IllegalArgumentException.class)
	public void buildEntityFromGetResponse_withInvalidType() {
		// Setup
		MultiGetItemResponse multiGetItemResponseMocked = mock(MultiGetItemResponse.class);
		GetResponse getResponseMocked = mock(GetResponse.class);
		when(getResponseMocked.isExists()).thenReturn(true);
		when(multiGetItemResponseMocked.getResponse()).thenReturn(getResponseMocked);

		// Action
		entityDao.buildEntityFromGetResponse(ESEntity.class, multiGetItemResponseMocked);
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
		when(deleteResponseMocked.isFound()).thenReturn(true);

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
		when(deleteResponseMocked.isFound()).thenReturn(false);

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
		when(listenableActionFutureMocked.actionGet()).thenThrow(new ElasticsearchException("Simulated exception"));

		// Action
		entityDao.delete(ESNode.class, 1l);
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
