package org.openstreetmap.osmosis.plugin.elasticsearch.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.EntityBuffer.EntityBufferFactory;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.Parameters;

public class WorkerPool {

	public static final int POLL_INTERVAL = 10;

	private final AtomicReference<EntityType> lastEntityType;
	private final BlockingQueue<Entity> taskQueue;
	private final List<Worker> workers;

	public WorkerPool(EntityDao entityDao, Parameters params) {
		this.lastEntityType = new AtomicReference<EntityType>();
		int queueSize = Integer.valueOf(params.getProperty(Parameters.CONFIG_QUEUE_SIZE));
		this.taskQueue = new ArrayBlockingQueue<Entity>(queueSize);
		int poolSize = Integer.valueOf(params.getProperty(Parameters.CONFIG_WORKER_POOL_SIZE));
		this.workers = new ArrayList<Worker>(poolSize);
		EntityBufferFactory factory = new EntityBufferFactory(entityDao, params);
		for (int i = 0; i < poolSize; i++) {
			String name = "Worker #" + i;
			Worker worker = new Worker(name, taskQueue, factory);
			workers.add(worker);
			worker.start();
		}
	}

	public synchronized void submit(Entity entity) {
		if (!entity.getType().equals(lastEntityType.getAndSet(entity.getType()))) {
			notifyNewType(entity.getType());
		}
		try {
			while (!taskQueue.offer(entity, POLL_INTERVAL, TimeUnit.MILLISECONDS));
		} catch (InterruptedException e) {
			throw new IllegalStateException("InterruptedException caught", e);
		}
	}

	protected void notifyNewType(EntityType type) {
		for (Worker worker : workers) {
			try {
				worker.notifyNewType(type);
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
