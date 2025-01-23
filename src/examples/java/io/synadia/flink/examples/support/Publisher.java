package io.synadia.flink.examples.support;

import io.nats.client.Connection;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Publisher implements Runnable {
    final Connection nc;
    final List<String> subjects;
    final AtomicInteger counter;
    final long delay;
    final AtomicBoolean keepGoing;

    public Publisher(Connection nc, List<String> subjects) throws IOException {
        this(nc, 500, subjects);
    }

    public Publisher(Connection nc, long delay, List<String> subjects) throws IOException {
        this.nc = nc;
        this.delay = delay;
        this.subjects = subjects;
        this.counter = new AtomicInteger();
        keepGoing = new AtomicBoolean(true);
    }

    public void stop() {
        keepGoing.set(false);
    }

    @Override
    public void run() {
        while (keepGoing.get()) {
            for (String subject : subjects) {
                int num = counter.incrementAndGet();
                String payload = "data-" + subject + "-" + num;
                nc.publish(subject, payload.getBytes());
                System.out.printf("Publishing. Subject: %s MessageRecord: %s\n", subject, payload);
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
