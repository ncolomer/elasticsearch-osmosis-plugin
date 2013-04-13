package org.openstreetmap.osmosis.plugin.elasticsearch.model.shape;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.shape.ESShape.ESShapeBuilder;

public class ESShapeTest {

	private ESShapeBuilder shapeBuilder;

	@Before
	public void setUp() throws Exception {
		shapeBuilder = new ESShapeBuilder();
	}

	@Test
	public void buildPoint() {
		// Setup
		shapeBuilder.addLocation(48.675881, 2.379247);

		// Action
		ESShape shape = shapeBuilder.build();

		// Assert
		Assert.assertEquals(ESShapeType.POINT, shape.getType());
		Assert.assertTrue(shape.isClosed());
		Assert.assertEquals(0, shape.getAreaKm2(), 1E-6);
		Assert.assertEquals(0, shape.getLenghtKm(), 1E-3);
		Assert.assertEquals(new ESLocation(48.675881, 2.379247), shape.getCentroid());
	}

	@Test
	public void buildLineString() {
		// Setup
		shapeBuilder.addLocation(48.675763, 2.379358).addLocation(48.675584, 2.379606).addLocation(48.675087, 2.380314)
				.addLocation(48.674958, 2.380947).addLocation(48.675093, 2.381405).addLocation(48.675406, 2.382000)
				.addLocation(48.675957, 2.383090).addLocation(48.676137, 2.383404).addLocation(48.676230, 2.384246)
				.addLocation(48.675890, 2.384684).addLocation(48.675580, 2.385125);

		// Action
		ESShape shape = shapeBuilder.build();

		// Assert
		Assert.assertEquals(ESShapeType.LINESTRING, shape.getType());
		Assert.assertFalse(shape.isClosed());
		Assert.assertEquals(0, shape.getAreaKm2(), 1E-6);
		Assert.assertEquals(721E-3, shape.getLenghtKm(), 1E-3);
		Assert.assertEquals(new ESLocation(48.67559909155728, 2.3822438099468224), shape.getCentroid());
	}

	@Test
	public void buildPolygon() {
		// Setup
		shapeBuilder.addLocation(48.6759473, 2.3792501).addLocation(48.6758837, 2.379149).addLocation(48.675816, 2.3792444)
				.addLocation(48.6758794, 2.3793465).addLocation(48.6759473, 2.3792501);

		// Action
		ESShape shape = shapeBuilder.build();

		// Assert
		Assert.assertEquals(ESShapeType.POLYGON, shape.getType());
		Assert.assertTrue(shape.isClosed());
		Assert.assertEquals(160E-6, shape.getAreaKm2(), 1E-6);
		Assert.assertEquals(52E-3, shape.getLenghtKm(), 1E-3);
		Assert.assertEquals(new ESLocation(48.67588161300993, 2.379247584621654), shape.getCentroid());
	}

}
