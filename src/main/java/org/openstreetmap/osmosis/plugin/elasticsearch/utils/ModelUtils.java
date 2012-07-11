package org.openstreetmap.osmosis.plugin.elasticsearch.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

public final class ModelUtils {

	public static Map<String, String> getTagsFromWay(Way way) {
		Map<String, String> tags = new HashMap<String, String>();
		for (Tag tag : way.getTags()) {
			tags.put(tag.getKey(), tag.getValue());
		}
		return tags;
	}

	public static List<Long> getNodesFromWay(Way way) {
		List<Long> nodes = new ArrayList<Long>();
		for (WayNode wayNode : way.getWayNodes()) {
			nodes.add(wayNode.getNodeId());
		}
		return nodes;
	}

	public static Object[] getLonLatFromNode(Node node) {
		return new Object[] { node.getLongitude(), node.getLatitude() };
	}

}
