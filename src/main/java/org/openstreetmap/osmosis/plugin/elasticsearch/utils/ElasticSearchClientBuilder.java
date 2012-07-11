package org.openstreetmap.osmosis.plugin.elasticsearch.utils;

import java.util.List;
import java.util.logging.Logger;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ElasticSearchClientBuilder {

	private static final Logger LOG = Logger.getLogger(ElasticSearchClientBuilder.class.getName());

	public String host;
	public int port;
	public String clusterName;

	public Client build() {
		Client client;
		// Node Client
		// Node node = NodeBuilder.nodeBuilder()
		// .clusterName(clusterName)
		// .client(true)
		// .node();
		// client = node.client();

		// Transport client
		LOG.info(String.format("Connecting to elasticsearch cluster '%s' via [%s:%s]",
				clusterName, host, port));
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", clusterName)
				.put("client.transport.sniff", true)
				.build();
		TransportClient transportClient = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(host, port));
		List<DiscoveryNode> nodes = transportClient.connectedNodes();
		if (nodes.size() == 0) throw new IllegalStateException("Unable to connect");
		LOG.info(String.format("Connected to %d node(s): %s", nodes.size(), nodes.toString()));
		client = transportClient;

		return client;
	}

}
