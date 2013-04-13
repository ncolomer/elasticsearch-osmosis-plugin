package org.openstreetmap.osmosis.plugin.elasticsearch.model.entity;

import junit.framework.Assert;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoShapeConstants;
import org.elasticsearch.index.query.GeoShapeFilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESEntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.AbstractElasticSearchInMemoryTest;

import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.impl.PointImpl;

public class ESNodeITest extends AbstractElasticSearchInMemoryTest {

	private static final String INDEX_NAME = "shape-test";

	private IndexAdminService indexAdminService;

	@Before
	public void setUp() throws Exception {
		indexAdminService = new IndexAdminService(client());
		String mappings = "{\"node\":{\"properties\":{\"shape\":{\"type\":\"geo_shape\"}}}}";
		indexAdminService.createIndex(INDEX_NAME, 1, 0, mappings);
	}

	@Test
	public void getNode() {
		// Setup
		ESNode node = ESNode.Builder.create().id(1854801716).location(48.675652, 2.384955)
				.addTag("highway", "traffic_signals").build();

		// Action
		indexAdminService.index(INDEX_NAME, ESEntityType.NODE.getIndiceName(), 1854801716l, node.toJson());
		refresh();

		// Assert
		GetResponse response = client().prepareGet(INDEX_NAME, ESEntityType.NODE.getIndiceName(), "1854801716").execute().actionGet();
		Assert.assertTrue(response.isExists());
		String expected = "{\"shape\":{\"type\":\"point\",\"coordinates\":" +
				"[2.384955,48.675652]},\"tags\":{\"highway\":\"traffic_signals\"}}";
		String actual = response.getSourceAsString();
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void searchNode_withGeoShape() {
		// Setup
		ESNode node = ESNode.Builder.create().id(1854801716).location(48.675652, 2.384955)
				.addTag("highway", "traffic_signals").build();
		indexAdminService.index(INDEX_NAME, ESEntityType.NODE.getIndiceName(), 1854801716l, node.toJson());
		refresh();

		// Action
		Shape shape = buildSquareShape(48.675652, 2.384955, 20);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.NODE.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setFilter(new GeoShapeFilterBuilder("shape", shape))
				.execute().actionGet();

		// Assert
		Assert.assertEquals(1, searchResponse.getHits().hits().length);
	}

	@Test
	public void searchNode_withGeoShape_shouldNotMatch() {
		// Setup
		ESNode node = ESNode.Builder.create().id(1854801716).location(48.675652, 2.384955)
				.addTag("highway", "traffic_signals").build();
		indexAdminService.index(INDEX_NAME, ESEntityType.NODE.getIndiceName(), 1854801716l, node.toJson());
		refresh();

		// Action
		Shape shape = buildSquareShape(48.676455, 2.380899, 20);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.NODE.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setFilter(new GeoShapeFilterBuilder("shape", shape))
				.execute().actionGet();

		// Assert
		Assert.assertEquals(0, searchResponse.getHits().hits().length);
	}

	@Test
	public void searchNode_withGeoShapeAndLargeFilterShape() {
		// Setup
		ESNode node = ESNode.Builder.create().id(1854801716).location(48.675652, 2.384955)
				.addTag("highway", "traffic_signals").build();
		indexAdminService.index(INDEX_NAME, ESEntityType.NODE.getIndiceName(), 1854801716l, node.toJson());
		refresh();

		// Action
		Shape shape = buildSquareShape(48.675652, 2.384955, 10000);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.NODE.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setFilter(new GeoShapeFilterBuilder("shape", shape))
				.execute().actionGet();

		// Assert
		Assert.assertEquals(1, searchResponse.getHits().hits().length);
	}

	@Test
	public void searchNode_withGeoShapeAndCloseFilterShape() {
		// Setup
		ESNode node = ESNode.Builder.create().id(1854801716).location(48.675652, 2.384955)
				.addTag("highway", "traffic_signals").build();
		indexAdminService.index(INDEX_NAME, ESEntityType.NODE.getIndiceName(), 1854801716l, node.toJson());
		refresh();

		// Action
		// Can't do better than 20m with default shape configuration
		Shape shape = buildSquareShape(48.675652, 2.384955, 20);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.NODE.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setFilter(new GeoShapeFilterBuilder("shape", shape))
				.execute().actionGet();

		// Assert
		Assert.assertEquals(1, searchResponse.getHits().hits().length);
	}

	protected Shape buildSquareShape(double centerLat, double centerLon, double distanceMeter) {
		Point point = new PointImpl(centerLon, centerLat, GeoShapeConstants.SPATIAL_CONTEXT);
		double radius = DistanceUtils.dist2Degrees(distanceMeter / 10E3, DistanceUtils.EARTH_MEAN_RADIUS_KM);
		return GeoShapeConstants.SPATIAL_CONTEXT.makeCircle(point, radius).getBoundingBox();
	}

}
