package org.openstreetmap.osmosis.plugin.elasticsearch.testutils;

import static junit.framework.Assert.fail;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.openstreetmap.osmosis.plugin.elasticsearch.model.entity.ESEntity;

/**
 * Provides an empty in-memory elasticsearch index
 * 
 * @author Nicolas Colomer
 */
public abstract class AbstractElasticSearchInMemoryTest {

	private static final String CLUSTER_NAME = "osm_test_cluster";

	private static File tmpFolder;
	private static Node node;

	@BeforeClass
	public static void setUpBeforeClass() throws IOException {
		tmpFolder = new File("tmp");
		String tmpFolderPath = tmpFolder.getCanonicalPath();
		FileUtils.deleteQuietly(tmpFolder);
		if (!tmpFolder.mkdir()) fail("Could not create a temporary folder [" + tmpFolderPath + "]");
		Settings settings = settingsBuilder()
				.put("cluster.name", CLUSTER_NAME)
				.put("gateway.type", "none")
				.put("index.store.type", "memory")
				.put("index.number_of_shards", 1)
				.put("index.number_of_replicas", 0)
				.put("path.data", tmpFolderPath)
				.build();
		node = NodeBuilder.nodeBuilder()
				.settings(settings)
				.node();
	}

	@AfterClass
	public static void tearDownAfterClass() throws IOException {
		node.close();
		FileUtils.deleteQuietly(tmpFolder);
	}

	@Before
	public void setUpTest() {
		delete();
	}

	protected Client client() {
		return node.client();
	}

	protected void delete(String... indices) {
		client().admin().indices().prepareDelete(indices).execute().actionGet();
	}

	protected void refresh(String... indices) {
		client().admin().indices().prepareRefresh(indices).execute().actionGet();
	}

	protected boolean exists(String... indices) {
		return client().admin().indices().prepareExists(indices).execute().actionGet().isExists();
	}

	protected void index(String index, ESEntity... entities) {
		for (ESEntity entity : entities) {
			index(index, entity.getType().getIndiceName(), entity.getIdString(), entity.toJson());
		}
	}

	protected void index(String index, String type, String id, String source) {
		client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
	}

	protected String clusterName() {
		return CLUSTER_NAME;
	}

	protected String nodeAddress() {
		NodesInfoResponse nodesInfo = client().admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet();
		String transportAddress = nodesInfo.getNodes()[0].getNode().address().toString();
		Matcher matcher = Pattern.compile("\\d{1,3}(?:\\.\\d{1,3}){3}(?::\\d{1,5})?").matcher(transportAddress);
		matcher.find();
		return matcher.group();
	}

}
