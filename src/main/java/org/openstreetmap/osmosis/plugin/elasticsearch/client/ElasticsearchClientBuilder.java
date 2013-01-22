package org.openstreetmap.osmosis.plugin.elasticsearch.client;

import java.util.logging.Logger;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class ElasticSearchClientBuilder {

	private static final Logger LOG = Logger.getLogger(ElasticSearchClientBuilder.class.getName());

	public String clusterName;
	public String hosts;

	private ElasticSearchClientBuilder() {}

	public static ElasticSearchClientBuilder newClient() {
		return new ElasticSearchClientBuilder();
	}

	public String getClusterName() {
		return clusterName;
	}

	public ElasticSearchClientBuilder setClusterName(String clusterName) {
		this.clusterName = clusterName;
		return this;
	}

	public String getHosts() {
		return hosts;
	}

	public ElasticSearchClientBuilder setHosts(String hosts) {
		this.hosts = hosts;
		return this;
	}

	public Client build() {
		// Build the elasticsearch client
		Client client = buildNodeClient();
		// Ensure client is connected
		ClusterHealthResponse health = client.admin().cluster()
				.health(new ClusterHealthRequest()).actionGet();
		if (health.getNumberOfDataNodes() == 0) throw new RuntimeException("Unable to connect to elasticsearch");
		LOG.info(String.format("Connected to %d data node(s) with cluster status %s",
				health.getNumberOfDataNodes(), health.getStatus().name()));
		return client;
	}

	protected Client buildNodeClient() {
		LOG.info(String.format("Connecting to elasticsearch cluster '%s' via [%s]" +
				" using NodeClient", clusterName, hosts));
		// Connect as NodeClient (member of the cluster), see Gists:
		// https://gist.github.com/2491022 and https://gist.github.com/2491022
		// http://www.elasticsearch.org/guide/reference/modules/discovery/zen.html
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("node.local", false) // Disable local JVM discovery
				.put("node.data", false) // Disable data on this node
				.put("node.master", false) // Never elected as master
				.put("node.client", true) // Various client optim
				.put("cluster.name", clusterName) // Join clusterName
				.put("discovery.type", "zen") // Use zen discovery
				// Connect to 1 master node min
				.put("discovery.zen.minimum_master_nodes", 1)
				// Disable multicast discovery
				.put("discovery.zen.ping.multicast.enabled", false)
				// Add one or more host to join
				.putArray("discovery.zen.ping.unicast.hosts", hosts.split(","))
				.build();
		Node node = NodeBuilder.nodeBuilder()
				.settings(settings)
				.node();
		return node.client();
	}

}
