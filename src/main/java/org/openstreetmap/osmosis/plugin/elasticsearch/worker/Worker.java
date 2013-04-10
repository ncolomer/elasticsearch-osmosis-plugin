package org.openstreetmap.osmosis.plugin.elasticsearch.worker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.EntityBuffer;
import org.openstreetmap.osmosis.plugin.elasticsearch.utils.EntityBuffer.EntityBufferFactory;

public class Worker extends Thread {

	private static final Logger LOG = Logger.getLogger(Worker.class.getName());

	private final BlockingQueue<Entity> taskQueue;
	private final EntityBufferFactory bufferFactory;
	private final AtomicReference<NewTypeNotification> newTypeNotification;

	private boolean running = true;

	public Worker(String name, BlockingQueue<Entity> taskQueue, EntityBufferFactory bufferFactory) {
		super(name);
		this.taskQueue = taskQueue;
		this.bufferFactory = bufferFactory;
		this.newTypeNotification = new AtomicReference<Worker.NewTypeNotification>();
	}

	@Override
	public void run() {
		Entity entity = null;
		EntityBuffer entityBuffer = null;
		NewTypeNotification notification = null;
		while (running || !taskQueue.isEmpty()) {
			try {
				// Check if a NewTypeNotification was triggered
				if ((notification = newTypeNotification.getAndSet(null)) != null) {
					LOG.fine("NewTypeNotification detected, flushing...");
					if (entityBuffer != null) entityBuffer.flush();
					entityBuffer = bufferFactory.buildForType(notification.getType());
					notification.getLatch().countDown();
				}
				// Poll the queue
				if ((entity = taskQueue.poll(WorkerPool.POLL_INTERVAL, TimeUnit.MILLISECONDS)) != null) {
					entityBuffer.add(entity);
				}
			} catch (InterruptedException e) {
				LOG.fine("InterruptedException triggered, leaving...");
			}
		}
		if (entityBuffer != null) entityBuffer.flush();
		LOG.fine(String.format("%s shutdown", getName()));
	}

	public void notifyNewType(EntityType type) throws InterruptedException {
		NewTypeNotification notification = new NewTypeNotification(type);
		newTypeNotification.set(notification);
		notification.getLatch().await();
	}

	public void shutdown() throws InterruptedException {
		running = false;
		join();
	}

	public class NewTypeNotification {

		private final CountDownLatch latch = new CountDownLatch(1);
		private final EntityType type;

		public NewTypeNotification(EntityType type) {
			this.type = type;
		}

		public CountDownLatch getLatch() {
			return latch;
		}

		public EntityType getType() {
			return type;
		}

	}

}
