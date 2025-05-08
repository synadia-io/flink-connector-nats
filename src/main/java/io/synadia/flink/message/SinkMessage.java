// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.message;

import io.nats.client.impl.Headers;

/**
 * This object represents data that will be used
 * for the message published by the Sink
 */
public class SinkMessage {
    public final byte[] payload;
    public final Headers headers;

    /**
     * Create a sink message with just a payload.
     * The payload can be empty or null, it's passed on to the message as is.
     * @param payload the payload
     */
    public SinkMessage(byte[] payload) {
        this.payload = payload;
        this.headers = null;
    }

    /**
     * Create a sink message with just a payload.
     * The payload can be empty or null, it's passed on to the message as is.
     * If the headers are empty or null, they will be ignored when creating the message.
     * @param payload the payload
     * @param headers the headers
     */
    public SinkMessage(byte[] payload, Headers headers) {
        this.payload = payload;
        this.headers = headers == null || headers.isEmpty() ? null : headers;
    }
}
