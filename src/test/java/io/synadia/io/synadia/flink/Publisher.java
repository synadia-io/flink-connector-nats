// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.io.synadia.flink;

import io.nats.client.Connection;
import io.nats.client.impl.Headers;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class Publisher implements Runnable {
    final Connection nc;
    final String[] subjects;
    final AtomicInteger counter;
    final long delay;
    final BiFunction<String, Integer, Headers> headerGenerator;
    final AtomicBoolean keepGoing;

    public Publisher(Connection nc, String... subjects) {
        this(nc, 200, null, subjects);
    }

    public Publisher(Connection nc, long delay, String... subjects) {
        this(nc, delay, null, subjects);
    }
    
    public Publisher(Connection nc, BiFunction<String, Integer, Headers> headerGenerator, String... subjects) {
        this(nc, 200, headerGenerator, subjects);
    }
    
    public Publisher(Connection nc, long delay, BiFunction<String, Integer, Headers> headerGenerator, String... subjects) {
        this.nc = nc;
        this.delay = delay;
        this.headerGenerator = headerGenerator;
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
            int num = counter.incrementAndGet();
            for (String subject : subjects) {
                if (headerGenerator == null) {
                    nc.publish(subject, dataString(subject, num).getBytes());
                }
                else {
                    nc.publish(subject, headerGenerator.apply(subject, num), dataString(subject, num).getBytes());
                }
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

    public static String dataString(String subject, Object num) {
        return "data-" + subject + "-" + num;
    }
}
