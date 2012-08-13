package org.openstreetmap.osmosis.plugin.elasticsearch.integration;

import static junit.framework.Assert.fail;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Provides an empty in-memory elasticsearch index
 * 
 * @author Nicolas Colomer
 */
public abstract class AbstractElasticsearchInMemoryTest {

	private static File tmpFolder;
	private static Node node;

	@BeforeClass
	public static void setUpBeforeClass() throws IOException {
		tmpFolder = new File("tmp");
		String tmpFolderName = tmpFolder.getCanonicalPath();
		if (tmpFolder.exists()) FileUtils.deleteDirectory(tmpFolder);
		if (!tmpFolder.mkdir()) fail("Could not create a temporary folder [" + tmpFolderName + "]");
		Settings settings = settingsBuilder()
				.put("index.store.type", "memory")
				.put("gateway.type", "none")
				.put("path.data", tmpFolderName)
				.build();
		node = NodeBuilder.nodeBuilder()
				.settings(settings)
				.local(true)
				.node();
	}

	@AfterClass
	public static void tearDownAfterClass() throws IOException {
		node.close();
		if (tmpFolder.exists()) FileUtils.deleteDirectory(tmpFolder);
	}

	protected Node node() {
		return node;
	}

	protected Client client() {
		return node.client();
	}

	protected void delete(String... indices) {
		client().admin().indices().prepareDelete(indices).execute().actionGet();
	}

	protected void refresh(String... indices) {
		client().admin().indices().refresh(Requests.refreshRequest(indices)).actionGet();
	}

}
