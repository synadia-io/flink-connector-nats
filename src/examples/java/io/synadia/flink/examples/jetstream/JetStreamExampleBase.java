// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.examples.jetstream;

import io.nats.client.api.StorageType;
import io.synadia.flink.examples.support.ExampleUtils;
import io.synadia.flink.utils.PropertiesUtils;

import java.io.IOException;
import java.util.Properties;

public class JetStreamExampleBase {

    public static final String PROPERTIES_FILE_PATH = "src/examples/resources/jetstream-example.properties";

    public static final Properties PROPS;
    public static final String SOURCE_A_STREAM;
    public static final String SOURCE_B_STREAM;
    public static final String SOURCE_A_SUBJECT;
    public static final String[] SOURCE_B_SUBJECTS;
    public static final int SOURCE_A_MESSAGE_COUNT;
    public static final int[] SOURCE_B_MESSAGE_COUNTS;
    public static final StorageType SOURCE_A_STORAGE_TYPE;
    public static final StorageType SOURCE_B_STORAGE_TYPE;
    public static final int SOURCES_TOTAL_SUBJECT_COUNT;
    public static final int SOURCES_TOTAL_MESSAGES;
    public static final int SOURCES_MOST_MESSAGES_ANY_SUBJECT;

    public static final String SINK_STREAM_NAME;
    public static final String SINK_SUBJECT;
    public static final StorageType SINK_STORAGE_TYPE;

    static {
        try {
            PROPS = PropertiesUtils.loadPropertiesFromFile(PROPERTIES_FILE_PATH);
            SOURCE_A_STREAM = PROPS.getProperty("source.a.stream");
            SOURCE_B_STREAM = PROPS.getProperty("source.b.stream");
            SOURCE_A_SUBJECT = PROPS.getProperty("source.a.subject");
            SOURCE_B_SUBJECTS = ExampleUtils.toStringArray(PROPS.getProperty("source.b.subjects"));
            SOURCE_A_MESSAGE_COUNT = Integer.parseInt(PROPS.getProperty("source.a.messages"));
            SOURCE_B_MESSAGE_COUNTS = ExampleUtils.toIntArray(PROPS.getProperty("source.b.messages"));
            SOURCE_A_STORAGE_TYPE = StorageType.get(PROPS.getProperty("source.a.storage.type"));
            SOURCE_B_STORAGE_TYPE = StorageType.get(PROPS.getProperty("source.b.storage.type"));

            SOURCES_TOTAL_SUBJECT_COUNT = SOURCE_A_MESSAGE_COUNT + SOURCE_B_MESSAGE_COUNTS.length;
            int total = SOURCE_A_MESSAGE_COUNT;
            int most = SOURCE_A_MESSAGE_COUNT;
            for (int count : SOURCE_B_MESSAGE_COUNTS) {
                total += count;
                most = Math.max(most, count);
            }
            SOURCES_TOTAL_MESSAGES = total;
            SOURCES_MOST_MESSAGES_ANY_SUBJECT = most;

            SINK_STREAM_NAME = PROPS.getProperty("sink.stream");
            SINK_SUBJECT = PROPS.getProperty("sink.subject");
            SINK_STORAGE_TYPE = StorageType.get(PROPS.getProperty("sink.storage.type"));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
   }
}