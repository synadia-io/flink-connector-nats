// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamOptions;
import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class ConnectionContext {
    public final Connection connection;
    public final JetStreamManagement jsm;
    public final JetStream js;

    public ConnectionContext(Connection connection, JetStreamOptions jso) throws IOException {
        this.connection = connection;
        this.jsm = connection.jetStreamManagement(jso);
        this.js = connection.jetStream(jso);
    }
}
