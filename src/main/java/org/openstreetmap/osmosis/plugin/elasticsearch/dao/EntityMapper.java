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
import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

public class EntityMapper {

	public XContentBuilder marshall(Entity entity) throws IOException {
		switch (entity.getType()) {
		case Node:
			Node node = (Node) entity;
			return jsonBuilder()
					.startObject()
					.field("location", new double[] { node.getLongitude(), node.getLatitude() })
					.field("tags", buildMapFromTags(node.getTags()))
					.endObject();
		case Way:
			Way way = (Way) entity;
			return jsonBuilder()
					.startObject()
					.field("tags", buildMapFromTags(way.getTags()))
					.field("nodes", buildListFromWayNodes(way.getWayNodes()))
					.endObject();
		case Relation:
			throw new UnsupportedOperationException("Unmarshall Relation is not yet supported");
		case Bound:
			throw new UnsupportedOperationException("Unmarshall Bound is not yet supported");
		default:
			throw new IllegalArgumentException("Unknown EntityType " + entity.getType());
		}
	}

	@SuppressWarnings("unchecked")
	public <T extends Entity> T unmarshall(EntityType type, GetResponse item) {
		switch (type) {
		case Node: {
			if (!item.getType().equals(EntityDao.NODE)) throw new IllegalArgumentException("Provided GetResponse is not a Node");
			Map<String, String> tags = (Map<String, String>) item.field("tags").getValue();
			List<Double> location = (List<Double>) item.field("location").getValue();
			CommonEntityData entityData = new CommonEntityData(Long.valueOf(item.getId()), 0, new Date(0), null, 0l, buildTagsFromMap(tags));
			return (T) new Node(entityData, location.get(1), location.get(0));
		}
		case Way: {
			if (!item.getType().equals(EntityDao.WAY)) throw new IllegalArgumentException("Provided GetResponse is not a Way");
			Map<String, String> tags = (Map<String, String>) item.field("tags").getValue();
			List<Object> nodes = (List<Object>) item.field("nodes").getValue();
			List<WayNode> wayNodes = buildWayNodesFromList(nodes);
			CommonEntityData entityData = new CommonEntityData(Long.valueOf(item.getId()), 0, new Date(0), null, 0l, buildTagsFromMap(tags));
			return (T) new Way(entityData, wayNodes);
		}
		case Relation:
			throw new UnsupportedOperationException("Unmarshall Relation is not yet supported");
		case Bound:
			throw new UnsupportedOperationException("Unmarshall Bound is not yet supported");
		default:
			throw new IllegalArgumentException("Unknown EntityType " + type);
		}
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