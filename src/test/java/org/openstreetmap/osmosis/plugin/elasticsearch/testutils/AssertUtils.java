package org.openstreetmap.osmosis.plugin.elasticsearch.testutils;

import java.util.Collection;
import java.util.Iterator;

import junit.framework.Assert;

import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

public class AssertUtils {

	public static void assertNodesEquals(Collection<Node> expected, Collection<Node> actual) {
		Assert.assertEquals(expected.size(), actual.size());
		Iterator<Node> expectedNodes = expected.iterator();
		Iterator<Node> actualNodes = actual.iterator();
		while (expectedNodes.hasNext() && actualNodes.hasNext()) {
			Node expectedNode = expectedNodes.next();
			Node actualNode = actualNodes.next();
			assertEquals(expectedNode, actualNode);
		}
	}

	public static void assertEquals(Node expected, Node actual) {
		Assert.assertEquals(expected.getId(), actual.getId());
		// Verify Location
		Assert.assertEquals(expected.getLatitude(), actual.getLatitude());
		Assert.assertEquals(expected.getLongitude(), actual.getLongitude());
		// Verify Tags
		Iterator<Tag> expectedTags = expected.getTags().iterator();
		Iterator<Tag> actualTags = actual.getTags().iterator();
		while (expectedTags.hasNext() && actualTags.hasNext()) {
			Tag expectedTag = expectedTags.next();
			Tag actualTag = actualTags.next();
			Assert.assertEquals(0, expectedTag.compareTo(actualTag));
		}
		Assert.assertFalse(expectedTags.hasNext() || actualTags.hasNext());
	}

	public static void assertWaysEquals(Collection<Way> expected, Collection<Way> actual) {
		Assert.assertEquals(expected.size(), actual.size());
		Iterator<Way> expectedWays = expected.iterator();
		Iterator<Way> actualWays = actual.iterator();
		while (expectedWays.hasNext() && actualWays.hasNext()) {
			Way expectedWay = expectedWays.next();
			Way actualWay = actualWays.next();
			assertEquals(expectedWay, actualWay);
		}
	}

	public static void assertEquals(Way expected, Way actual) {
		Assert.assertEquals(expected.getId(), actual.getId());
		// Verify Tags
		Iterator<Tag> expectedTags = expected.getTags().iterator();
		Iterator<Tag> actualTags = actual.getTags().iterator();
		while (expectedTags.hasNext() && actualTags.hasNext()) {
			Tag expectedTag = expectedTags.next();
			Tag actualTag = actualTags.next();
			Assert.assertEquals(0, expectedTag.compareTo(actualTag));
		}
		Assert.assertFalse(expectedTags.hasNext() || actualTags.hasNext());
		// Verify WayNodes
		Iterator<WayNode> expectedWayNodes = expected.getWayNodes().iterator();
		Iterator<WayNode> actualWayNodes = actual.getWayNodes().iterator();
		while (expectedWayNodes.hasNext() && actualWayNodes.hasNext()) {
			WayNode expectedWayNode = expectedWayNodes.next();
			WayNode actualWayNode = actualWayNodes.next();
			Assert.assertEquals(0, expectedWayNode.compareTo(actualWayNode));
		}
		Assert.assertFalse(expectedWayNodes.hasNext() || actualWayNodes.hasNext());
	}

}
