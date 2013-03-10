package org.openstreetmap.osmosis.plugin.elasticsearch.utils;

import org.elasticsearch.client.Client;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.service.IndexAdminService;

public class Endpoint {

	private final Client client;
	private final IndexAdminService indexAdminService;
	private final EntityDao entityDao;

	public Endpoint(Client client, IndexAdminService indexAdminService, EntityDao entityDao) {
		this.client = client;
		this.indexAdminService = indexAdminService;
		this.entityDao = entityDao;
	}

	public Client getClient() {
		return client;
	}

	public IndexAdminService getIndexAdminService() {
		return indexAdminService;
	}

	public EntityDao getEntityDao() {
		return entityDao;
	}

}
