package io.synadia.flink.examples.support;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;

public class ExampleConnectionListener implements ConnectionListener {
    @Override
    public void connectionEvent(Connection conn, Events type) {
        System.out.printf("Connection event: %s\n", type);
    }
}
