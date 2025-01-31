package io.synadia.flink.examples.support;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Publisher implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Publisher.class);

    final Connection nc;
    final JetStream js;
    final List<String> subjects;
    final boolean noisy;
    final long delayJitter;
    final int messageCountJitter;
    final AtomicInteger counter;
    final AtomicBoolean keepGoing;

    public Publisher(Connection nc, List<String> subjects) throws IOException {
        this(nc, null, subjects, true, 500, 1);
    }

    public Publisher(Connection nc, List<String> subjects, boolean noisy, long delayJitter, int messageCountJitter) throws IOException {
        this(nc, null, subjects, noisy, delayJitter, messageCountJitter);
    }

    public Publisher(JetStream js, List<String> subjects) throws IOException {
        this(null, js, subjects, true, 500, 1);
    }

    public Publisher(JetStream js, List<String> subjects, boolean noisy, long delayJitter, int messageCountJitter) throws IOException {
        this(null, js, subjects, noisy, delayJitter, messageCountJitter);
    }

    Publisher(Connection nc, JetStream js, List<String> subjects, boolean noisy, long delayJitter, int messageCountJitter) throws IOException {
        this.nc = nc;
        this.js = js;
        this.subjects = subjects;
        this.noisy = noisy;
        this.delayJitter = delayJitter;
        this.messageCountJitter = messageCountJitter;
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
                int count = messageCountJitter < 2 ? 1 : ThreadLocalRandom.current().nextInt(messageCountJitter) + 1;
                for (int c = 0; c < count; c++) {
                    int num = counter.incrementAndGet();
                    String payload = makePayload(subject, num);
                    if (js == null) {
                        nc.publish(subject, payload.getBytes());
                    }
                    else {
                        try {
                            js.publish(subject, payload.getBytes());
                        }
                        catch (Exception e) {
                            // this should never really happen during an example run
                            throw new RuntimeException(e);
                        }
                    }
                    if (noisy) {
                        LOG.info("Publishing. Subject: {} MessageRecord: {}", subject, payload);
                    }
                }
            }
            try {
                //noinspection BusyWait
                Thread.sleep(delayJitter);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static String makePayload(String subject, int num) {
        return "data--" + subject + "--" + num;
    }

    public static String extractSubject(String data) {
        int at1 = data.indexOf("--");
        int at2 = data.lastIndexOf("--");
        return data.substring(at1 + 2, at2);
    }
}
