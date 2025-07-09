// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.examples.support;

import io.nats.client.Connection;
import io.nats.client.impl.AckType;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;

// =====================================================================================================================================
// This class is used to simulate acknowledge messages in a Flink job that processes & acks NATS messages.
// It connects to a NATS server and sends an acknowledgment message back to the replyTo subject specified in the input JSON.
// The input JSON is expected to have a "data" field containing the message data and a "reply_to" field specifying where to send the ack.
// =====================================================================================================================================
public class AckMapFunction extends RichMapFunction<String, String> {
    private static final byte[] ACK_BODY_BYTES = AckType.AckAck.bodyBytes(-1);
    private transient Connection natsCtx;

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.natsCtx = ExampleUtils.connect(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE);
        }
        catch (Exception e){
            System.out.println("Error connecting to NATS server: " + e);
            throw new FlinkRuntimeException("Error connecting to NATS server", e);
        }
    }

    @Override
    public String map(String s) throws Exception {
        try {
            JsonValue parsed = JsonParser.parse(s);
            JsonValue data = parsed.map.get("data");
            if (data == null) {
                throw new IllegalArgumentException("data field is missing in the input: " + s);
            }

            JsonValue replyTo = parsed.map.get("reply_to");
            if (replyTo == null) {
                throw new IllegalArgumentException("reply_to is missing in the input: " + s);
            }

            natsCtx.publish(replyTo.string, ACK_BODY_BYTES);
            return data.string;
        }
        catch (Exception e) {
            System.out.println("Error processing message: " + e);
            throw new FlinkRuntimeException("Error processing message: " + s, e);
        }
    }

    @Override
    public void close() throws Exception {
        if (natsCtx != null) {
            natsCtx.close();
        }
    }
}
