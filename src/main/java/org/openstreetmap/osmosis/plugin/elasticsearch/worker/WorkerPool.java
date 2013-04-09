package org.openstreetmap.osmosis.plugin.elasticsearch.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;

public class WorkerPool {

	private final Queue<Entity> taskQueue;
	private final List<Worker> workers;

	public WorkerPool(int poolSize, final EntityDao entityDao) {
		this.taskQueue = new ConcurrentLinkedQueue<Entity>();
		this.workers = new ArrayList<Worker>();

		for (int i = 0; i < poolSize; i++) {
			String name = "Worker #" + i;
			Worker worker = new Worker(name, entityDao, taskQueue);
			workers.add(worker);
			worker.start();
		}
	}

	public void submit(Entity entity) {
		taskQueue.offer(entity);
	}

	public void prepareNewEntityType(EntityType type, int size) {
		for (Worker worker : workers) {
			try {
				worker.prepareNewEntityType(type, size);
			} catch (InterruptedException e) {}
		}
	}

	public void shutdown() {
		for (Worker worker : workers) {
			try {
				worker.shutdown();
			} catch (InterruptedException e) {}
		}
	}

}
