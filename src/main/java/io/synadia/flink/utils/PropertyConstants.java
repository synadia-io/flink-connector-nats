// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

public interface PropertyConstants {
    // ===================================================================================
    // Property Name Constants
    // -----------------------------------------------------------------------------------
    // Some are also reused from io.nats.client.support.ApiConstants. See the Readme
    // ===================================================================================
    String CONNECT_JITTER = "connect_jitter";
    String PAYLOAD_DESERIALIZER = "payload_deserializer";
    String PAYLOAD_SERIALIZER = "payload_serializer";

    String JSO_REQUEST_TIMEOUT = "jso_request_timeout";
    String JSO_PREFIX = "jso_prefix";
    String JSO_DOMAIN = "jso_domain";

    String FINISHED = "finished";
    String LAST_REPLY_TO = "last_reply_to";
    String LAST_EMITTED_SEQ = "last_emitted_seq";
    String CONSUME_OPTIONS = "consume_options";
    String ACK = "ack";
    String START_SEQ = "start_seq";
    String JETSTREAM_SUBJECT_CONFIGURATIONS = "jetstream_subject_configurations";

    // ===================================================================================
    // Property Value Constants
    // ===================================================================================
    String STRING_PAYLOAD_SERIALIZER_CLASSNAME = "io.synadia.flink.payload.StringPayloadSerializer";
    String STRING_PAYLOAD_DESERIALIZER_CLASSNAME = "io.synadia.flink.payload.StringPayloadDeserializer";
    String BYTE_ARRAY_PAYLOAD_SERIALIZER_CLASSNAME = "io.synadia.flink.payload.ByteArrayPayloadSerializer";
    String BYTE_ARRAY_PAYLOAD_DESERIALIZER_CLASSNAME = "io.synadia.flink.payload.ByteArrayPayloadDeserializer";
}
