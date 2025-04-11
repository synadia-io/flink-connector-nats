// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.examples;

import io.synadia.flink.payload.StringPayloadDeserializer;
import io.synadia.flink.source.JetStreamSource;
import io.synadia.flink.source.JetStreamSourceBuilder;
import io.synadia.flink.source.JetStreamSubjectConfiguration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static io.synadia.flink.examples.JetStreamExampleHelper.*;

public abstract class ConfigurationFileGenerationExample {
    // ==========================================================================================
    // Example Configuration: Use these settings to change how the JetStreamSource is configured
    // ==========================================================================================

    // ACK false means use an ordered consumer with no acking
    // ACK true means the split(s) will ack (AckPolicy.All) messages at the checkpoint
    // Try false or true
    public static final boolean ACK = true;

    // <= 0 makes the source Boundedness.CONTINUOUS_UNBOUNDED
    // > 0 makes the source Boundedness.BOUNDED by giving it a maximum number of messages to read
    // Try 0 or 50000
    public static final int MAX_MESSAGES_TO_READ = 50000;

    // ==========================================================================================
    // Set these to where you want example files to end up...
    // ==========================================================================================
    public static final String OUTPUT_JSON_FILE = "C:\\temp\\JetStreamSourceConfig.json";
    public static final String OUTPUT_YAML_FILE = "C:\\temp\\JetStreamSourceConfig.yaml";

    public static void main(String[] args) throws IOException {
        // ==========================================================================================
        // These configurations are identical to what is in the JetStreamExample
        // See that code for details...
        // ==========================================================================================
        JetStreamSubjectConfiguration subjectConfigurationA = JetStreamSubjectConfiguration.builder()
            .streamName(SOURCE_A_STREAM)
            .maxMessagesToRead(MAX_MESSAGES_TO_READ)
            .ack(ACK)
            .buildWithSubject(SOURCE_A_SUBJECT);

        List<JetStreamSubjectConfiguration> subjectConfigurationsB = JetStreamSubjectConfiguration.builder()
            .streamName(SOURCE_B_STREAM)
            .maxMessagesToRead(MAX_MESSAGES_TO_READ)
            .ack(ACK)
            .buildWithSubjects(SOURCE_B_SUBJECTS);

        JetStreamSource<String> source = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(CONNECTION_PROPS)
            .payloadDeserializer(new StringPayloadDeserializer())
            .addSubjectConfigurations(subjectConfigurationA)
            .addSubjectConfigurations(subjectConfigurationsB)
            .build();

        writeToFile(OUTPUT_JSON_FILE, source.toJson());
        writeToFile(OUTPUT_YAML_FILE, source.toYaml());
    }

    public static void writeToFile(String file, String text) throws IOException {
        System.out.println("Writing to " + file);
        System.out.println(text);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(text);
        writer.flush();
        writer.close();
        System.out.println();
    }
}
