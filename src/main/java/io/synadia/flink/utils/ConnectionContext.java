// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

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
