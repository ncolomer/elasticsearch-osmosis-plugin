package org.openstreetmap.osmosis.plugin.elasticsearch.utils;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.OsmUser;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

public class OsmDataBuilder {

	public static Node buildSampleNode() {
		List<Tag> tags = Arrays.asList(new Tag[] { new Tag("highway", "traffic_signals") });
		CommonEntityData entityData = new CommonEntityData(1l, 0, new Date(), new OsmUser(1, "nco"), 1l, tags);
		return new Node(entityData, 1.0d, 2.0d);
	}

	public static Way buildSampleWay() {
		List<Tag> tags = Arrays.asList(new Tag[] { new Tag("highway", "residential") });
		CommonEntityData entityData = new CommonEntityData(1l, 0, new Date(), new OsmUser(1, "nco"), 1l, tags);
		return new Way(entityData, Arrays.asList(new WayNode[] { new WayNode(1l) }));
	}

}
