package io.synadia.io.synadia.flink;

import io.nats.client.Connection;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Publisher implements Runnable {
    final Connection nc;
    final String[] subjects;
    final AtomicInteger count;
    final long delay;
    final AtomicBoolean keepGoing;

    public Publisher(Connection nc, String... subjects) {
        this(nc, 200, subjects);
    }

    public Publisher(Connection nc, long delay, String... subjects) {
        this.nc = nc;
        this.delay = delay;
        this.subjects = subjects;
        this.count = new AtomicInteger();
        keepGoing = new AtomicBoolean(true);
    }

    public void stop() {
        keepGoing.set(false);
    }

    @Override
    public void run() {
        while (keepGoing.get()) {
            for (String subject : subjects) {
                int num = count.incrementAndGet();
                nc.publish(subject, ("data-" + subject + "-" + num).getBytes());
            }
            try {
                //noinspection BusyWait
                Thread.sleep(delay);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
