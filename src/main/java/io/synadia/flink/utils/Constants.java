// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

import io.nats.client.support.ApiConstants;

public interface Constants {
    String SUBJECTS = "subjects";
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
    String CONFIG = io.nats.client.support.ApiConstants.CONFIG;
    String MSGS = io.nats.client.support.ApiConstants.MSGS;
    String STREAM_NAME = io.nats.client.support.ApiConstants.STREAM_NAME;
    String SUBJECT = io.nats.client.support.ApiConstants.SUBJECT;
    String START_SEQ = "start_seq";
    String START_TIME = ApiConstants.START_TIME;
    String MAX_MSGS = io.nats.client.support.ApiConstants.MAX_MSGS;

    String JETSTREAM_SUBJECT_CONFIGURATIONS = "jetstream_subject_configurations";
}
