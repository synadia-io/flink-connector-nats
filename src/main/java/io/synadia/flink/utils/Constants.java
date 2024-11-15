// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

import org.apache.flink.connector.base.source.reader.SourceReaderOptions;

public interface Constants {
    String NATS_PREFIX = "nats.";
    String SOURCE_PREFIX = "source.";
    String SINK_PREFIX = "sink.";

    String SUBJECTS = "subjects";
    String STARTUP_JITTER_MIN = "startup.jitter.min";
    String STARTUP_JITTER_MAX = "startup.jitter.max";
    String PAYLOAD_DESERIALIZER = "payload.deserializer";
    String PAYLOAD_SERIALIZER = "payload.serializer";

    String READER_ELEMENT_QUEUE_CAPACITY = "reader.element.queue.capacity";
    int DEFAULT_ELEMENT_QUEUE_CAPACITY = SourceReaderOptions.ELEMENT_QUEUE_CAPACITY.defaultValue();

    String FETCH_ONE_MESSAGE_TIMEOUT = "fetch.one.timeout";
    long DEFAULT_FETCH_ONE_MESSAGE_TIMEOUT_MS = 1000;

    String MAX_FETCH_RECORDS = "max.fetch.records";
    int DEFAULT_MAX_FETCH_RECORDS = 100;

    String FETCH_TIMEOUT = "fetch.timeout";
    long DEFAULT_FETCH_TIMEOUT_MS = 1000;

    String AUTO_ACK_INTERVAL = "auto.ack.interval";
    long DEFAULT_AUTO_ACK_INTERVAL_MS = 5000;

    String ENABLE_AUTO_ACK = "enable.auto.ack";
    boolean DEFAULT_ENABLE_AUTO_ACK = false;
}
