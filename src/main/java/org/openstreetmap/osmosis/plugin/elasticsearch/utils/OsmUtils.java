package org.openstreetmap.osmosis.plugin.elasticsearch.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

public final class OsmUtils {

	public static Map<String, String> getTags(Way way) {
		return buildMapFromTags(way.getTags());
	}

	public static Map<String, String> getTags(Node node) {
		return buildMapFromTags(node.getTags());
	}

	protected static Map<String, String> buildMapFromTags(Collection<Tag> tags) {
		Map<String, String> map = new HashMap<String, String>();
		for (Tag tag : tags) {
			map.put(tag.getKey(), tag.getValue());
		}
		return map;
	}

	public static List<Long> getNodes(Way way) {
		List<Long> nodes = new ArrayList<Long>();
		for (WayNode wayNode : way.getWayNodes()) {
			nodes.add(wayNode.getNodeId());
		}
		return nodes;
	}

}
