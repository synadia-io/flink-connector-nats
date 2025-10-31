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
    /**
     * The connection
     */
    public final Connection connection;

    /**
     * The JetStreamManagement context
     */
    public final JetStreamManagement jsm;

    /**
     * The JetStream context
     */
    public final JetStream js;

    /**
     * Construct a ConnectionContext
     * @param connection the connection
     * @param jso the JetStreamOptions used to make the JetStreamManagement and JetStream contexts
     * @throws IOException if there is an IO exception creating the JetStreamManagement context
     */
    public ConnectionContext(Connection connection, JetStreamOptions jso) throws IOException {
        this.connection = connection;
        this.jsm = connection.jetStreamManagement(jso);
        this.js = jsm.jetStream();
    }
}
