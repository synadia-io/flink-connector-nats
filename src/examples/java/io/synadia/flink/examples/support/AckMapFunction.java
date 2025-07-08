// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.examples.support;

import io.nats.client.Connection;
import io.nats.client.impl.AckType;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class AckMapFunction extends RichMapFunction<String, String> {
    private static final byte[] ACK_BODY_BYTES = AckType.AckAck.bodyBytes(-1);
    private transient Connection natsCtx;

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.natsCtx = ExampleUtils.connect(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE);
        } catch (Exception e) {
            System.out.println("Failed to connect: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public String map(String s) throws Exception {
        String[] splits = s.split("\\|");
        String data = splits[0].trim();
        String replyTo = splits[1].trim();

        try {
            natsCtx.publish(replyTo, ACK_BODY_BYTES);
        } catch (Exception e) {
            System.out.println("Failed to send ACK: " + e.getMessage());
            throw e;
        }

        return data;
    }

    @Override
    public void close() throws Exception {
        if (natsCtx != null) {
            natsCtx.close();
        }
    }
}