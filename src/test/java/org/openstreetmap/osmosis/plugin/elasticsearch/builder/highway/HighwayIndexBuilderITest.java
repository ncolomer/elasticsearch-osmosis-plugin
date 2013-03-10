package org.openstreetmap.osmosis.plugin.elasticsearch.builder.highway;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoShapeConstants;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.index.query.GeoShapeFilterBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.builder.AbstractIndexBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.AbstractElasticSearchInMemoryTest;
import org.openstreetmap.osmosis.plugin.elasticsearch.testutils.OsmDataBuilder;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Endpoint;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Parameters;

import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.impl.PointImpl;

public class HighwayIndexBuilderITest extends AbstractElasticSearchInMemoryTest {

	private static final String INDEX_NAME = "osm-test";

	private EntityDao entityDao;

	private AbstractIndexBuilder indexBuilder;

	@Before
	public void setUp() throws IOException {
		IndexAdminService indexAdminService = new IndexAdminService(client());
		entityDao = new EntityDao(INDEX_NAME, client());
		Endpoint endpoint = new Endpoint(client(), indexAdminService, entityDao);
		Parameters params = new Parameters.Builder().loadResource("plugin.properties").build();
		indexBuilder = new HighwayIndexBuilder(endpoint, params);
		indexAdminService.createIndex(INDEX_NAME, 1, 0, params.getProperty(Parameters.INDEX_MAPPINGS));
		indexAdminService.createIndex(indexBuilder.getSpecializedIndexName(), 1, 0, params.getProperty("highway.mappings"));
	}

	@Test
	public void buildIndex() {
		// Setup
		buildMainIndexSample();

		// Action
		indexBuilder.buildIndex();
		refresh(indexBuilder.getSpecializedIndexName());

		// Assert
		GetResponse response = client().prepareGet(indexBuilder.getSpecializedIndexName(), "way", "1").execute().actionGet();
		Assert.assertTrue(response.exists());
		String expected = "{\"tags\":{\"highway\":\"tertiary\",\"name\":\"Avenue Jean Galmot\"},\"line\":{\"type\":\"linestring\"," +
				"\"coordinates\":[[-52.3294249,4.9316988],[-52.3289426,4.9318342],[-52.3284144,4.9319625],[-52.3278228,4.9321291]]}}";
		String actual = response.getSourceAsString();
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void geoShapeFilterSearch_shouldMatch() {
		// Setup
		buildMainIndexSample();
		indexBuilder.buildIndex();
		refresh();

		Point point = new PointImpl(-52.32866311271843, 4.931903377671382, GeoShapeConstants.SPATIAL_CONTEXT);
		double radius = DistanceUtils.dist2Degrees(0.020, DistanceUtils.EARTH_MEAN_RADIUS_KM);
		Rectangle rectangle = GeoShapeConstants.SPATIAL_CONTEXT.makeCircle(point, radius).getBoundingBox();

		// Action
		SearchRequestBuilder queryBuilder = client().prepareSearch(indexBuilder.getSpecializedIndexName()).setTypes("way")
				.setQuery(QueryBuilders.matchAllQuery())
				.setFilter(new GeoShapeFilterBuilder("line", rectangle).relation(ShapeRelation.INTERSECTS));
		System.out.println(queryBuilder.toString());
		SearchResponse searchResponse = queryBuilder.execute().actionGet();

		// Assert
		Assert.assertEquals(1, searchResponse.hits().hits().length);
	}

	@Test
	public void geoShapeFilterSearch_withFarShapeLocation_shouldNotMatch() {
		// Setup
		buildMainIndexSample();
		indexBuilder.buildIndex();
		refresh();

		Point point = new PointImpl(-50, 4, GeoShapeConstants.SPATIAL_CONTEXT);
		double radius = DistanceUtils.dist2Degrees(0.020, DistanceUtils.EARTH_MEAN_RADIUS_KM);
		Rectangle rectangle = GeoShapeConstants.SPATIAL_CONTEXT.makeCircle(point, radius).getBoundingBox();

		// Action
		SearchRequestBuilder queryBuilder = client().prepareSearch(indexBuilder.getSpecializedIndexName())
				.setTypes("way")
				.setQuery(QueryBuilders.matchAllQuery())
				.setFilter(new GeoShapeFilterBuilder("line", rectangle).relation(ShapeRelation.INTERSECTS));
		System.out.println(queryBuilder.toString());
		SearchResponse searchResponse = queryBuilder.execute().actionGet();

		// Assert
		Assert.assertTrue(searchResponse.hits().hits().length == 0);
	}

	@Test
	public void geoShapeFilterSearch_withTooSmallShape_shouldNotMatch() {
		// Setup
		buildMainIndexSample();
		indexBuilder.buildIndex();
		refresh();

		Point point = new PointImpl(-52.32866311271843, 4.931903377671382, GeoShapeConstants.SPATIAL_CONTEXT);
		double radius = DistanceUtils.dist2Degrees(0.001, DistanceUtils.EARTH_MEAN_RADIUS_KM);
		Rectangle rectangle = GeoShapeConstants.SPATIAL_CONTEXT.makeCircle(point, radius).getBoundingBox();

		// Action
		SearchRequestBuilder queryBuilder = client().prepareSearch(indexBuilder.getSpecializedIndexName())
				.setTypes("way")
				.setQuery(QueryBuilders.matchAllQuery())
				.setFilter(new GeoShapeFilterBuilder("line", rectangle).relation(ShapeRelation.INTERSECTS));
		System.out.println(queryBuilder.toString());
		SearchResponse searchResponse = queryBuilder.execute().actionGet();

		// Assert
		Assert.assertTrue(searchResponse.hits().hits().length == 0);
	}

	/**
	 * Throws TooManyClauses exception
	 */
	@Test
	@Ignore
	public void geoShapeQuerySearch_shouldMatch() {
		// Setup
		buildMainIndexSample();
		indexBuilder.buildIndex();
		refresh();

		Point point = new PointImpl(-52.32866311271843, 4.931903377671382, GeoShapeConstants.SPATIAL_CONTEXT);
		double radius = DistanceUtils.dist2Degrees(0.020, DistanceUtils.EARTH_MEAN_RADIUS_KM);
		Rectangle rectangle = GeoShapeConstants.SPATIAL_CONTEXT.makeCircle(point, radius).getBoundingBox();

		// Action
		SearchRequestBuilder queryBuilder = client().prepareSearch(indexBuilder.getSpecializedIndexName())
				.setTypes("way")
				.setQuery(new GeoShapeQueryBuilder("line", rectangle).relation(ShapeRelation.INTERSECTS));
		System.out.println(queryBuilder.toString());
		SearchResponse searchResponse = queryBuilder.execute().actionGet();

		// Assert
		Assert.assertTrue(searchResponse.hits().hits().length == 1);
	}

	/**
	 * Throws TooManyClauses exception
	 */
	@Test
	@Ignore
	public void geoShapeQuerySearch_shouldNotMatch() {
		// Setup
		buildMainIndexSample();
		indexBuilder.buildIndex();
		refresh();

		Point point = new PointImpl(-50, 4, GeoShapeConstants.SPATIAL_CONTEXT);
		double radius = DistanceUtils.dist2Degrees(0.020, DistanceUtils.EARTH_MEAN_RADIUS_KM);
		Rectangle rectangle = GeoShapeConstants.SPATIAL_CONTEXT.makeCircle(point, radius).getBoundingBox();

		// Action
		SearchRequestBuilder queryBuilder = client().prepareSearch(indexBuilder.getSpecializedIndexName())
				.setTypes("way")
				.setQuery(new GeoShapeQueryBuilder("line", rectangle).relation(ShapeRelation.INTERSECTS));
		System.out.println(queryBuilder.toString());
		SearchResponse searchResponse = queryBuilder.execute().actionGet();

		// Assert
		Assert.assertTrue(searchResponse.hits().hits().length == 0);
	}

	private void buildMainIndexSample() {
		Node node1 = OsmDataBuilder.buildNode(1l, 4.9316988, -52.3294249);
		Node node2 = OsmDataBuilder.buildNode(2l, 4.9318342, -52.3289426);
		Node node3 = OsmDataBuilder.buildNode(3l, 4.9319625, -52.3284144);
		Node node4 = OsmDataBuilder.buildNode(4l, 4.9321291, -52.3278228);
		Way way = OsmDataBuilder.buildWay(1l, Arrays.asList(new WayNode[] { new WayNode(1l), new WayNode(2l), new WayNode(3l),
				new WayNode(4l) }), new Tag("highway", "tertiary"), new Tag("name", "Avenue Jean Galmot"));
		entityDao.saveAll(Arrays.asList(new Entity[] { node1, node2, node3, node4, way }));
		refresh(INDEX_NAME);
	}

}
