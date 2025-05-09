// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

public interface Constants {

    // ===================================================================================
    // JetStreamOptions Property Name Constants
    // ===================================================================================
    String JSO_PREFIX = "jso_prefix";
    String JSO_DOMAIN = "jso_domain";

    // ===================================================================================
    // Sink and Source JSON / YAML Configuration Field Name Constants
    // ===================================================================================
    String SOURCE_CONVERTER_CLASS_NAME = "source_converter_class_name";
    String SINK_CONVERTER_CLASS_NAME = "sink_converter_class_name";
    String JETSTREAM_SUBJECT_CONFIGURATIONS = "jetstream_subject_configurations";
    String SUBJECTS = "subjects"; // Used in core source and both core and js sink

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
    String ASCII_STRING_SINK_CONVERTER_CLASSNAME = "io.synadia.flink.message.AsciiStringSinkConverter";
    String ASCII_STRING_SOURCE_CONVERTER_CLASSNAME = "io.synadia.flink.message.AsciiStringSourceConverter";
    String UTF8_STRING_SINK_CONVERTER_CLASSNAME = "io.synadia.flink.message.Utf8StringSinkConverter";
    String UTF8_STRING_SOURCE_CONVERTER_CLASSNAME = "io.synadia.flink.message.Utf8StringSourceConverter";
    String BYTE_ARRAY_SINK_CONVERTER_CLASSNAME = "io.synadia.flink.message.ByteArraySinkConverter";
    String BYTE_ARRAY_SOURCE_CONVERTER_CLASSNAME = "io.synadia.flink.message.ByteArraySourceConverter";

    // ===================================================================================
    // Split state JSON Field Constants
    // ===================================================================================
    String FINISHED = "finished";
    String LAST_REPLY_TO = "last_reply_to";
    String LAST_EMITTED_SEQ = "last_emitted_seq";
    String MESSAGES = "messages";
    String SUBJECT_CONFIG = "subject_config";
}
