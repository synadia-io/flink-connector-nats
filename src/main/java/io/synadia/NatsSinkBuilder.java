// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import io.nats.client.support.Validator;
import io.synadia.payload.PayloadSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static io.synadia.Constants.SINK_CONNECTION_PROPERTIES_FILE;

/**
 * Builder to construct {@link NatsSink}.
 *
 * <p>The following example shows the minimum setup to create a NatsSink that writes String values
 * to one or more NATS subjects.
 *
 * <pre>{@code
 * NatsSink<String> sink = NatsSink
 *     .<String>builder
 *     .setSubjects("subject1", "subject2")
 *     .setPropertiesFile("/path/to/jnats_client_connection.properties")
 *     .build();
 * }</pre>
 *
 * @see NatsSink
 * @param <InputT> type of the records written to Kafka
 */
public class NatsSinkBuilder<InputT> {
    private List<String> subjects;
    private Properties properties;
    private String propertiesFile;
    private PayloadSerializer<InputT> payloadSerializer;

    /**
     * Set one or more subjects for the sink. Replaces all subjects previously set in the builder.
     * @param subjects the subjects
     * @return the builder
     */
    public NatsSinkBuilder<InputT> setSubjects(String... subjects) {
        this.subjects = subjects == null || subjects.length == 0 ? null : Arrays.asList(subjects);
        return this;
    }

    /**
     * Set the subjects for the sink. Replaces all subjects previously set in the builder.
     * @param subjects the list of subjects
     * @return the builder
     */
    public NatsSinkBuilder<InputT> setSubjects(List<String> subjects) {
        if (subjects == null || subjects.isEmpty()) {
            this.subjects = null;
        }
        else {
            this.subjects = new ArrayList<>(subjects);
        }
        return this;
    }

    /**
     * Set the properties used to instantiate the {@link io.nats.client.Connection Connection}
     * <p>The properties should include enough information to create a connection to a NATS server.
     * It can directly have properties from {@link io.nats.client.Options Connection Options}
     * Or it can have a {@value Constants#SINK_CONNECTION_PROPERTIES_FILE} key so the properties can be read from the server executing the job.</p>
     * @param properties the properties
     * @return the builder
     */
    public NatsSinkBuilder<InputT> setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Set the {@value Constants#SINK_CONNECTION_PROPERTIES_FILE}
     * @param propertiesFile the properties file path that would be available on all servers executing the job.
     * @return the builder
     */
    public NatsSinkBuilder<InputT> setPropertiesFile(String propertiesFile) {
        this.propertiesFile = propertiesFile;
        return this;
    }

    /**
     * Set the payload serializer for the sink.
     * @param payloadSerializer the serializer.
     * @return the builder
     */
    public NatsSinkBuilder<InputT> payloadSerializer(PayloadSerializer<InputT> payloadSerializer) {
        this.payloadSerializer = payloadSerializer;
        return this;
    }

    /**
     * Build a NatsSink. Subject and
     * @return the sink
     */
    public NatsSink<InputT> build() {
        Validator.required(subjects, "Sink Subject(s)");
        if (propertiesFile == null) {
            Validator.required(properties, "Sink Properties");
        }
        else {
            properties = new Properties();
            properties.setProperty(SINK_CONNECTION_PROPERTIES_FILE, propertiesFile);
        }
        Validator.required(payloadSerializer, "Payload Serializer");
        return new NatsSink<>(subjects, properties, payloadSerializer);
    }
}
