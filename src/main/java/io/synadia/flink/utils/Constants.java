// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

/**
 * Constants for the user
 */
public interface Constants {

    // ===================================================================================
    // JetStreamOptions Property Name Constants
    // ===================================================================================

    /**
     * Property for JetStreamOptions prefix 
     */
    String JSO_PREFIX = "jso_prefix";

    /**
     * Property for JetStreamOptions domain 
     */
    String JSO_DOMAIN = "jso_domain";

    // ===================================================================================
    // Sink and Source JSON / YAML Configuration Field Name Constants
    // ===================================================================================

    /**
     * Source JSON / YAML Configuration Field Name Constants
     */
    String SOURCE_CONVERTER_CLASS_NAME = "source_converter_class_name";

    /**
     * Sink JSON / YAML Configuration Field Name Constants
     */
    String SINK_CONVERTER_CLASS_NAME = "sink_converter_class_name";

    /**
     * Sink and Source JSON / YAML Configuration Field Name Constants
     */
    String JETSTREAM_SUBJECT_CONFIGURATIONS = "jetstream_subject_configurations";

    /**
     * Sink and Source JSON / YAML Configuration Field Name Constants
     */
    String SUBJECTS = "subjects"; // Used in core source and both core and js sink

    // ===================================================================================
    // JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constants
    // ===================================================================================

    /**
     * JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constant
     */
    String STREAM_NAME = "stream_name";

    /**
     * JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constant
     */
    String SUBJECT = "subject";

    /**
     * JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constant
     */
    String DURABLE_NAME = "durable_name";

    /**
     * JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constant
     */
    String CONSUMER_NAME_PREFIX = "consumer_name_prefix";

    /**
     * JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constant
     */
    String INACTIVE_THRESHOLD = "inactive_threshold";

    /**
     * JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constant
     */
    String START_SEQUENCE = "start_sequence";

    /**
     * JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constant
     */
    String START_TIME = "start_time";

    /**
     * JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constant
     */
    String MAX_MESSAGES_TO_READ = "max_messages_to_read";

    /**
     * JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constant
     */
    String ACK_BEHAVIOR = "ack_behavior";

    /**
     * JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constant
     */
    String ACK_WAIT = "ack_wait";

    /**
     * JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constant
     */
    String BATCH_SIZE = "batch_size";

    /**
     * JetStreamSubjectConfiguration JSON / YAML Configuration Field Name Constant
     */
    String THRESHOLD_PERCENT = "threshold_percent";

    // ===================================================================================
    // Sink and Source JSON / YAML Configuration Value Constants
    // ===================================================================================

    /**
     * Sink JSON / YAML Configuration Value Constant
     */
    String ASCII_STRING_SINK_CONVERTER_CLASSNAME = "io.synadia.flink.message.AsciiStringSinkConverter";

    /**
     * Source JSON / YAML Configuration Value Constant
     */
    String ASCII_STRING_SOURCE_CONVERTER_CLASSNAME = "io.synadia.flink.message.AsciiStringSourceConverter";

    /**
     * Sink JSON / YAML Configuration Value Constant
     */
    String UTF8_STRING_SINK_CONVERTER_CLASSNAME = "io.synadia.flink.message.Utf8StringSinkConverter";

    /**
     * Source JSON / YAML Configuration Value Constant
     */
    String UTF8_STRING_SOURCE_CONVERTER_CLASSNAME = "io.synadia.flink.message.Utf8StringSourceConverter";

    /**
     * Sink JSON / YAML Configuration Value Constant
     */
    String BYTE_ARRAY_SINK_CONVERTER_CLASSNAME = "io.synadia.flink.message.ByteArraySinkConverter";

    /**
     * Source JSON / YAML Configuration Value Constant
     */
    String BYTE_ARRAY_SOURCE_CONVERTER_CLASSNAME = "io.synadia.flink.message.ByteArraySourceConverter";

    // ===================================================================================
    // Split state JSON Field Constants
    // ===================================================================================

    /**
     * Split state JSON Field Constant 
     */
    String FINISHED = "finished";

    /**
     * Split state JSON Field Constant 
     */
    String LAST_REPLY_TO = "last_reply_to";

    /**
     * Split state JSON Field Constant 
     */
    String LAST_EMITTED_SEQ = "last_emitted_seq";

    /**
     * Split state JSON Field Constant 
     */
    String MESSAGES = "messages";

    /**
     * Split state JSON Field Constant 
     */
    String SUBJECT_CONFIG = "subject_config";
}
