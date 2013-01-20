package org.openstreetmap.osmosis.plugin.elasticsearch.dao;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

public class EntityMapper {

	public XContentBuilder marshallNode(Node node) throws IOException {
		return jsonBuilder()
				.startObject()
				.field("location", new double[] { node.getLongitude(), node.getLatitude() })
				.field("tags", buildMapFromTags(node.getTags()))
				.endObject();
	}

	public Node unmarshallNode(GetResponse get) {
		if (!get.getType().equals("node")) throw new IllegalArgumentException("Provided get type " + get.getType() + " is not a node");
		long id = Long.valueOf(get.getId());
		@SuppressWarnings("unchecked")
		Map<String, String> tags = (Map<String, String>) get.field("tags").getValue();
		@SuppressWarnings("unchecked")
		List<Double> location = (List<Double>) get.field("location").getValue();
		CommonEntityData entityData = new CommonEntityData(id, 0, new Date(0), null, 0l, buildTagsFromMap(tags));
		Node node = new Node(entityData, location.get(1), location.get(0));
		return node;
	}

	public Node unmarshallNode(SearchHit hit) {
		if (!hit.getType().equals("node")) throw new IllegalArgumentException("Provided hit type " + hit.getType() + " is not a node");
		long id = Long.valueOf(hit.getId());
		Map<String, String> tags = hit.field("tags").getValue();
		List<Double> location = hit.field("location").getValue();
		CommonEntityData entityData = new CommonEntityData(id, 0, new Date(0), null, 0l, buildTagsFromMap(tags));
		Node node = new Node(entityData, location.get(1), location.get(0));
		return node;
	}

	public XContentBuilder marshallWay(Way way) throws IOException {
		return jsonBuilder()
				.startObject()
				.field("tags", buildMapFromTags(way.getTags()))
				.field("nodes", buildListFromWayNodes(way.getWayNodes()))
				.endObject();
	}

	public Way unmarshallWay(GetResponse get) {
		if (!get.getType().equals("way")) throw new IllegalArgumentException("Provided get type " + get.getType() + " is not a way");
		long id = Long.valueOf(get.getId());
		@SuppressWarnings("unchecked")
		Map<String, String> tags = (Map<String, String>) get.field("tags").getValue();
		@SuppressWarnings("unchecked")
		List<Object> nodes = (List<Object>) get.field("nodes").getValue();
		List<WayNode> wayNodes = buildWayNodesFromList(nodes);
		CommonEntityData entityData = new CommonEntityData(id, 0, new Date(0), null, 0l, buildTagsFromMap(tags));
		Way way = new Way(entityData, wayNodes);
		return way;
	}

	public Way unmarshallWay(SearchHit hit) {
		if (!hit.getType().equals("way")) throw new IllegalArgumentException("Provided hit type " + hit.getType() + " is not a way");
		long id = Long.valueOf(hit.getId());
		Map<String, String> tags = hit.field("tags").getValue();
		List<WayNode> wayNodes = buildWayNodesFromList(hit.field("nodes").getValues());
		CommonEntityData entityData = new CommonEntityData(id, 0, new Date(0), null, 0l, buildTagsFromMap(tags));
		Way way = new Way(entityData, wayNodes);
		return way;
	}

	protected Map<String, String> buildMapFromTags(Collection<Tag> tags) {
		Map<String, String> map = new HashMap<String, String>();
		for (Tag tag : tags) {
			map.put(tag.getKey(), tag.getValue());
		}
		return map;
	}

	protected Collection<Tag> buildTagsFromMap(Map<String, String> map) {
		Collection<Tag> collection = new ArrayList<Tag>();
		for (String key : map.keySet()) {
			collection.add(new Tag(key, map.get(key)));
		}
		return collection;
	}

	protected List<Long> buildListFromWayNodes(List<WayNode> wayNodes) {
		List<Long> nodes = new ArrayList<Long>();
		for (WayNode wayNode : wayNodes) {
			nodes.add((Long) wayNode.getNodeId());
		}
		return nodes;
	}

	protected List<WayNode> buildWayNodesFromList(List<Object> list) {
		List<WayNode> wayNodes = new ArrayList<WayNode>();
		for (Object nodeId : list) {
			wayNodes.add(new WayNode(Long.valueOf(nodeId.toString())));
		}
		return wayNodes;
	}

}