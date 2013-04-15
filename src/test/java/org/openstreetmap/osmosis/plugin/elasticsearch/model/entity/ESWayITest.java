package org.openstreetmap.osmosis.plugin.elasticsearch.model.entity;

import java.util.HashMap;

import junit.framework.Assert;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoShapeConstants;
import org.elasticsearch.index.query.GeoShapeFilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.AbstractElasticSearchInMemoryTest;

import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.impl.PointImpl;

public class ESWayITest extends AbstractElasticSearchInMemoryTest {

	private static final String INDEX_NAME = "shape-test";

	private IndexAdminService indexAdminService;

	@Before
	public void setUp() throws Exception {
		indexAdminService = new IndexAdminService(client());
		HashMap<String, String> mappings = new HashMap<String, String>();
		mappings.put(ESEntityType.WAY.getIndiceName(), "{\"properties\":{\"centroid\":{\"type\":\"geo_point\"},\"shape\":{\"type\":\"geo_shape\"}}}");
		indexAdminService.createIndex(INDEX_NAME, 1, 0, mappings);
	}

	@Test
	public void getWay() {
		// Setup
		ESWay way = ESWay.Builder.create().id(40849832l)
				.addLocation(48.675763, 2.379358).addLocation(48.675584, 2.379606).addLocation(48.675288, 2.379955)
				.addTag("highway", "residential").addTag("name", "Avenue Marc Sangnier").build();

		// Action
		index(INDEX_NAME, way);
		refresh();

		// Assert
		GetResponse response = client().prepareGet(INDEX_NAME, ESEntityType.WAY.getIndiceName(), "40849832").execute().actionGet();
		Assert.assertTrue(response.isExists());
		String expected = "{\"centroid\":[2.37966091923039,48.67553114382843],\"length\":0.08489436252741311," +
				"\"area\":0.0,\"shape\":{\"type\":\"linestring\",\"coordinates\":" +
				"[[2.379358,48.675763],[2.379606,48.675584],[2.379955,48.675288]]}," +
				"\"tags\":{\"highway\":\"residential\",\"name\":\"Avenue Marc Sangnier\"}}";
		String actual = response.getSourceAsString();
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void getClosedWay() {
		// Setup
		ESWay way = ESWay.Builder.create().id(97583115l)
				.addLocation(48.67581, 2.379255).addLocation(48.675874, 2.379358).addLocation(48.675946, 2.379262)
				.addLocation(48.675885, 2.379161).addLocation(48.67581, 2.379255)
				.addTag("building", "yes").build();

		// Action
		indexAdminService.index(INDEX_NAME, ESEntityType.WAY.getIndiceName(), 97583115l, way.toJson());
		refresh();

		// Assert
		GetResponse response = client().prepareGet(INDEX_NAME, ESEntityType.WAY.getIndiceName(), "97583115").execute().actionGet();
		Assert.assertTrue(response.isExists());
		String expected = "{\"centroid\":[2.3792591400498715,48.6758784828737],\"length\":0.053319107940731796," +
				"\"area\":1.661088030797273E-4,\"shape\":{\"type\":\"polygon\",\"coordinates\":" +
				"[[[2.379255,48.67581],[2.379358,48.675874],[2.379262,48.675946],[2.379161,48.675885],[2.379255,48.67581]]]}," +
				"\"tags\":{\"building\":\"yes\"}}";
		String actual = response.getSourceAsString();
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void searchWay_withGeoShape() {
		// Setup
		ESWay way = ESWay.Builder.create().id(40849832l)
				.addLocation(48.675763, 2.379358).addLocation(48.675584, 2.379606).addLocation(48.675288, 2.379955)
				.addTag("highway", "residential").addTag("name", "Avenue Marc Sangnier").build();
		indexAdminService.index(INDEX_NAME, ESEntityType.WAY.getIndiceName(), 40849832l, way.toJson());
		refresh();

		// Action
		Shape shape = buildSquareShape(48.675763, 2.379358, 20);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.WAY.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setFilter(new GeoShapeFilterBuilder("shape", shape))
				.execute().actionGet();

		// Assert
		Assert.assertEquals(1, searchResponse.getHits().hits().length);
	}

	@Test
	public void searchWay_withGeoShape_shouldNotMatch() {
		// Setup
		ESWay way = ESWay.Builder.create().id(40849832l)
				.addLocation(48.675763, 2.379358).addLocation(48.675584, 2.379606).addLocation(48.675288, 2.379955)
				.addTag("highway", "residential").addTag("name", "Avenue Marc Sangnier").build();
		indexAdminService.index(INDEX_NAME, ESEntityType.WAY.getIndiceName(), 40849832l, way.toJson());
		refresh();

		// Action
		Shape shape = buildSquareShape(48.676455, 2.380899, 20);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.WAY.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setFilter(new GeoShapeFilterBuilder("shape", shape))
				.execute().actionGet();

		// Assert
		Assert.assertEquals(0, searchResponse.getHits().hits().length);
	}

	@Test
	public void searchWay_withGeoShapeAndLargeFilterShape() {
		// Setup
		ESWay way = ESWay.Builder.create().id(40849832l)
				.addLocation(48.675763, 2.379358).addLocation(48.675584, 2.379606).addLocation(48.675288, 2.379955)
				.addTag("highway", "residential").addTag("name", "Avenue Marc Sangnier").build();
		indexAdminService.index(INDEX_NAME, ESEntityType.WAY.getIndiceName(), 40849832l, way.toJson());
		refresh();

		// Action
		Shape shape = buildSquareShape(48.675763, 2.379358, 10000);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.WAY.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setFilter(new GeoShapeFilterBuilder("shape", shape))
				.execute().actionGet();

		// Assert
		Assert.assertEquals(1, searchResponse.getHits().hits().length);
	}

	@Test
	public void searchWay_withGeoShapeAndLargeIndexedShape() {
		// Setup
		// This way is ~ 550 meters large
		ESWay way = ESWay.Builder.create().id(40849832l)
				.addLocation(48.675763, 2.379358).addLocation(48.675584, 2.379606).addLocation(48.675087, 2.380314)
				.addLocation(48.674958, 2.380947).addLocation(48.675093, 2.381405).addLocation(48.675406, 2.382000)
				.addLocation(48.675957, 2.383090).addLocation(48.676137, 2.383404).addLocation(48.676230, 2.384246)
				.addLocation(48.675890, 2.384684).addLocation(48.675580, 2.385125)
				.addTag("highway", "residential").build();
		indexAdminService.index(INDEX_NAME, ESEntityType.WAY.getIndiceName(), 40849832l, way.toJson());
		refresh();

		// Action
		// ~ 45 meters min shape radius to match
		// center between positions index #5 and #6
		Shape shape = buildSquareShape(48.675689, 2.38259, 45);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.WAY.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setFilter(new GeoShapeFilterBuilder("shape", shape))
				.execute().actionGet();

		// Assert
		Assert.assertEquals(1, searchResponse.getHits().hits().length);
	}

	@Test
	public void searchClosedWay_withGeoShape() {
		// Setup
		ESWay way = ESWay.Builder.create().id(97583115l)
				.addLocation(48.67581, 2.379255).addLocation(48.675874, 2.379358).addLocation(48.675946, 2.379262)
				.addLocation(48.675885, 2.379161).addLocation(48.67581, 2.379255)
				.addTag("building", "yes").build();
		indexAdminService.index(INDEX_NAME, ESEntityType.WAY.getIndiceName(), 97583115l, way.toJson());
		refresh();

		// Action
		Shape shape = buildSquareShape(48.675763, 2.379358, 100);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.WAY.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setFilter(new GeoShapeFilterBuilder("shape", shape))
				.execute().actionGet();

		// Assert
		Assert.assertEquals(1, searchResponse.getHits().hits().length);
	}

	@Test
	public void searchClosedWay_withGeoShape_shouldNotMatch() {
		// Setup
		ESWay way = ESWay.Builder.create().id(97583115l)
				.addLocation(48.67581, 2.379255).addLocation(48.675874, 2.379358).addLocation(48.675946, 2.379262)
				.addLocation(48.675885, 2.379161).addLocation(48.67581, 2.379255)
				.addTag("building", "yes").build();
		indexAdminService.index(INDEX_NAME, ESEntityType.WAY.getIndiceName(), 97583115l, way.toJson());
		refresh();

		// Action
		Shape shape = buildSquareShape(48.676455, 2.380899, 20);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.WAY.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setFilter(new GeoShapeFilterBuilder("shape", shape))
				.execute().actionGet();

		// Assert
		Assert.assertEquals(0, searchResponse.getHits().hits().length);
	}

	protected Shape buildSquareShape(double centerLat, double centerLon, double distanceMeter) {
		Point point = new PointImpl(centerLon, centerLat, GeoShapeConstants.SPATIAL_CONTEXT);
		double radius = DistanceUtils.dist2Degrees(distanceMeter / 10E3, DistanceUtils.EARTH_MEAN_RADIUS_KM);
		return GeoShapeConstants.SPATIAL_CONTEXT.makeCircle(point, radius).getBoundingBox();
	}

}
