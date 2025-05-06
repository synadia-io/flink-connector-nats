// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

public interface Constants {

    // ===================================================================================
    // Connection Property Name Constants
    // ===================================================================================
    String CONNECT_JITTER = "connect_jitter";
    String JSO_REQUEST_TIMEOUT = "jso_request_timeout";
    String JSO_PREFIX = "jso_prefix";
    String JSO_DOMAIN = "jso_domain";

    // ===================================================================================
    // Sink and Source JSON / YAML Configuration Field Name Constants
    // ===================================================================================
    String PAYLOAD_DESERIALIZER = "payload_deserializer";
    String PAYLOAD_SERIALIZER = "payload_serializer";
    String JETSTREAM_SUBJECT_CONFIGURATIONS = "jetstream_subject_configurations";

    // ===================================================================================
    // JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constants
    // ===================================================================================
    String STREAM_NAME = "stream_name";
    String SUBJECT = "subject";
    String START_SEQUENCE = "start_sequence";
    String START_TIME = "start_time";
    String MAX_MESSAGES_TO_READ = "max_messages_to_read";
    String ACK_MODE = "ack_mode";
    String BATCH_SIZE = "batch_size";
    String THRESHOLD_PERCENT = "threshold_percent";

    // ===================================================================================
    // Sink and Source JSON / YAML Configuration Value Constants
    // ===================================================================================
    String STRING_PAYLOAD_SERIALIZER_CLASSNAME = "io.synadia.flink.payload.StringPayloadSerializer";
    String STRING_PAYLOAD_DESERIALIZER_CLASSNAME = "io.synadia.flink.payload.StringPayloadDeserializer";
    String BYTE_ARRAY_PAYLOAD_SERIALIZER_CLASSNAME = "io.synadia.flink.payload.ByteArrayPayloadSerializer";
    String BYTE_ARRAY_PAYLOAD_DESERIALIZER_CLASSNAME = "io.synadia.flink.payload.ByteArrayPayloadDeserializer";

    // ===================================================================================
    // Split state JSON Field Constants
    // ===================================================================================
    String FINISHED = "finished";
    String LAST_REPLY_TO = "last_reply_to";
    String LAST_EMITTED_SEQ = "last_emitted_seq";
    String MESSAGES = "messages";
    String SUBJECT_CONFIG = "subject_config";
}
