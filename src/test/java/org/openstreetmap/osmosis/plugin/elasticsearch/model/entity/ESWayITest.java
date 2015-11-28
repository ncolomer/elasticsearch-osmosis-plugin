package org.openstreetmap.osmosis.plugin.elasticsearch.model.entity;

import java.util.HashMap;

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.impl.PointImpl;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.AbstractElasticSearchInMemoryTest;

import org.elasticsearch.common.geo.GeoUtils;
import static org.elasticsearch.common.geo.builders.ShapeBuilder.SPATIAL_CONTEXT;

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
		String expected = "{\"centroid\":[2.37966091923039,48.67553114382843],\"lengthKm\":0.08489436252741311," +
				"\"areaKm2\":0.0,\"shape\":{\"type\":\"linestring\",\"coordinates\":" +
				"[[2.379358,48.675763],[2.379606,48.675584],[2.379955,48.675288]]}," +
				"\"tags\":{\"name\":\"Avenue Marc Sangnier\",\"highway\":\"residential\"}}";
		String actual = response.getSourceAsString();
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void getClosedWay() {
		// Setup
		ESWay expectedWay = ESWay.Builder.create().id(97583115l)
				.addLocation(48.67581, 2.379255).addLocation(48.675874, 2.379358).addLocation(48.675946, 2.379262)
				.addLocation(48.675885, 2.379161).addLocation(48.67581, 2.379255)
				.addTag("building", "yes").build();

		// Action
		indexAdminService.index(INDEX_NAME, ESEntityType.WAY.getIndiceName(), 97583115l, expectedWay.toJson());
		refresh();

		// Assert
		GetResponse response = client().prepareGet(INDEX_NAME, ESEntityType.WAY.getIndiceName(), "97583115").execute().actionGet();
		Assert.assertTrue(response.isExists());
             
                // Instead of comparing strings, assert a real way object,
                // The original assert (equal strings) Fails because of rounding differences for areaKm2:
                // expected: areaKm2":1.661088030[79727]3E-4 > but was: areaKm2":1.661088030[80079]3E-4
                // Thas why I decided to parse the response to a way object.
                
                ESWay actualWay = ESWay.Builder.buildFromGetReponse(response);
                // Since we set the id on insert, test if it is equal
                Assert.assertEquals(expectedWay.getId(), actualWay.getId());
                Assert.assertEquals(expectedWay.getShapeType(), actualWay.getShapeType());
                Assert.assertArrayEquals(expectedWay.getCoordinates(), actualWay.getCoordinates());
                // Centroid should be equal
                Assert.assertEquals(expectedWay.getCentroid(), actualWay.getCentroid());
                // Are the tags equal?
                Assert.assertEquals(expectedWay.getTags(), actualWay.getTags());
                // Is the area equal? Might have to set a tolerance
                Assert.assertEquals(expectedWay.getArea(), actualWay.getArea(), 0);
                Assert.assertEquals(expectedWay.getLenght(), actualWay.getLenght(),0);
                //System.out.println(response.getSource());
                
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
		ShapeBuilder shape = buildSquareShape(48.675763, 2.379358, 20);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.WAY.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setPostFilter(new GeoShapeQueryBuilder("shape", shape))
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
        ShapeBuilder shape = buildSquareShape(48.676455, 2.380899, 20);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.WAY.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setPostFilter(new GeoShapeQueryBuilder("shape", shape))
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
        ShapeBuilder shape = buildSquareShape(48.675763, 2.379358, 10000);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.WAY.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setPostFilter(new GeoShapeQueryBuilder("shape", shape))
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
        ShapeBuilder shape = buildSquareShape(48.675689, 2.38259, 45);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.WAY.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setPostFilter(new GeoShapeQueryBuilder("shape", shape))
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
		ShapeBuilder shape = buildSquareShape(48.675763, 2.379358, 100);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.WAY.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setPostFilter(new GeoShapeQueryBuilder("shape", shape))
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
		ShapeBuilder shape = buildSquareShape(48.676455, 2.380899, 20);
		SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setTypes(ESEntityType.WAY.getIndiceName())
				.setQuery(QueryBuilders.matchAllQuery())
				.setPostFilter(new GeoShapeQueryBuilder("shape", shape))
				.execute().actionGet();

		// Assert
		Assert.assertEquals(0, searchResponse.getHits().hits().length);
	}

	protected ShapeBuilder buildSquareShape(double centerLat, double centerLon, double distanceMeter) {
		Point point = new PointImpl(centerLon, centerLat, SPATIAL_CONTEXT);
		Rectangle shape = SPATIAL_CONTEXT.makeCircle(point, 360 * distanceMeter / GeoUtils.EARTH_EQUATOR).getBoundingBox();
		return ShapeBuilder.newEnvelope().bottomRight(shape.getMaxX(), shape.getMinY()).topLeft(shape.getMinX(), shape.getMaxY());
	}

}
