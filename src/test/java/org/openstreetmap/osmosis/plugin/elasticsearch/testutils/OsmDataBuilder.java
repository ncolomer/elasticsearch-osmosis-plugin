package org.openstreetmap.osmosis.plugin.elasticsearch.testutils;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.OsmUser;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.ESNode;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.ESWay;

public class OsmDataBuilder {

	public static Node buildSampleNode(long id) {
		List<Tag> tags = Arrays.asList(new Tag[] { new Tag("highway", "traffic_signals") });
		CommonEntityData entityData = new CommonEntityData(id, 0, new Date(), new OsmUser(1, "nco"), 1l, tags);
		return new Node(entityData, 1.0d, 2.0d);
	}

	public static Node buildSampleNode() {
		return buildSampleNode(1);
	}

	public static Way buildSampleWay(long id) {
		List<Tag> tags = Arrays.asList(new Tag[] { new Tag("highway", "residential") });
		CommonEntityData entityData = new CommonEntityData(id, 0, new Date(), new OsmUser(1, "nco"), 1l, tags);
		return new Way(entityData, Arrays.asList(new WayNode[] { new WayNode(1l) }));
	}

	public static Way buildSampleWay() {
		return buildSampleWay(1);
	}

	// ESEntity

	public static ESNode buildSampleESNode(long id) {
		return ESNode.Builder.create().id(id).location(1.0d, 2.0d).addTag("highway", "traffic_signals").build();
	}

	public static ESNode buildSampleESNode() {
		return buildSampleESNode(1);
	}

	public static ESWay buildSampleESWay(long id) {
		return ESWay.Builder.create().id(id).addLocation(1.0d, 2.0d).addTag("highway", "residential").build();
	}

	public static ESWay buildSampleESWay() {
		return buildSampleESWay(1);
	}

}
