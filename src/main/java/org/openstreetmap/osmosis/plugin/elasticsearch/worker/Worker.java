package org.openstreetmap.osmosis.plugin.elasticsearch.worker;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.dao.EntityDao;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.EntityBuffer;

public class Worker extends Thread {

	private static final Logger LOG = Logger.getLogger(Worker.class.getName());
	private static final int POLL_INTERVAL = 10;

	private final EntityDao entityDao;
	private final Queue<Entity> taskQueue;
	private final AtomicReference<NewEntityType> newEntityType;

	private boolean running = true;
	private EntityBuffer entityBuffer;

	public Worker(String name, EntityDao entityDao, Queue<Entity> taskQueue) {
		super(name);
		this.entityDao = entityDao;
		this.taskQueue = taskQueue;
		this.newEntityType = new AtomicReference<Worker.NewEntityType>();
	}

	@Override
	public void run() {
		Entity entity;
		NewEntityType newType;
		while (running || !taskQueue.isEmpty()) {
			if ((entity = taskQueue.poll()) != null) {
				LOG.fine(String.format("%s received Entity %s", getName(), entity));
				entityBuffer.add(entity);
			} else if ((newType = newEntityType.getAndSet(null)) != null) {
				if (entityBuffer != null) entityBuffer.flush();
				entityBuffer = new EntityBuffer(entityDao, newType.size);
				LOG.fine(String.format("%s ready to consume %s", getName(), newType.type));
				newType.latch.countDown();
			} else {
				try {
					sleep(POLL_INTERVAL);
				} catch (InterruptedException e) {}
			}

		}
		entityBuffer.flush();
		LOG.fine(String.format("%s shutdown", getName()));
	}

	public void prepareNewEntityType(EntityType type, int size) throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		newEntityType.set(new NewEntityType(type, size, latch));
		latch.await();
	}

	public void shutdown() throws InterruptedException {
		running = false;
		interrupt();
		join();
	}

	private class NewEntityType {

		private EntityType type;
		private int size;
		private CountDownLatch latch;

		public NewEntityType(EntityType type, int size, CountDownLatch latch) {
			this.type = type;
			this.size = size;
			this.latch = latch;
		}

	}

}
