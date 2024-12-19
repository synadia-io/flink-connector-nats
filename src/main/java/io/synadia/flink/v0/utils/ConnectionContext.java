package io.synadia.flink.v0.utils;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamOptions;

import java.io.IOException;

public class ConnectionContext {
    public final Connection connection;
    public final JetStreamManagement jsm;
    public final JetStream js;

    public ConnectionContext(Connection connection, JetStreamOptions jso) throws IOException {
        this.connection = connection;
        this.jsm = connection.jetStreamManagement(jso);
        this.js = jsm.jetStream();
    }
}
