// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink;

import io.synadia.flink.payload.PayloadSerializer;
import io.synadia.flink.utils.BuilderBase;
import io.synadia.flink.utils.Constants;

import java.util.List;
import java.util.Properties;

/**
 * Builder to construct {@link JetStreamSink}.
 *
 * <p>The following example shows the minimum setup to create a NatsSink that writes String values
 * to one or more NATS subjects.
 *
 * <pre>{@code
 * JetStreamSink<String> sink = JetStreamSink
 *     .<String>builder
 *     .subjects("subject1", "subject2")
 *     .connectionPropertiesFile("/path/to/jnats_client_connection.properties")
 *     .build();
 * }</pre>
 *
 * @see NatsSink
 * @param <InputT> type of the records written
 */
public class JetStreamSinkBuilder<InputT> extends BuilderBase<InputT, JetStreamSinkBuilder<InputT>> {

    public JetStreamSinkBuilder() {
        super(true, true);
    }

    @Override
    protected JetStreamSinkBuilder<InputT> getThis() {
        return this;
    }

    /**
     * Set one or more subjects for the sink. Replaces all subjects previously set in the builder.
     * @param subjects the subjects
     * @return the builder
     */
    public JetStreamSinkBuilder<InputT> subjects(String... subjects) {
        return super.subjects(subjects);
    }

    /**
     * Set the subjects for the sink. Replaces all subjects previously set in the builder.
     * @param subjects the list of subjects
     * @return the builder
     */
    public JetStreamSinkBuilder<InputT> subjects(List<String> subjects) {
        return super.subjects(subjects);
    }

    /**
     * Set the payload serializer for the sink.
     * @param payloadSerializer the serializer.
     * @return the builder
     */
    public JetStreamSinkBuilder<InputT> payloadSerializer(PayloadSerializer<InputT> payloadSerializer) {
        return super.payloadSerializer(payloadSerializer);
    }

    /**
     * Set the fully qualified name of the desired class payload serializer for the sink.
     * @param payloadSerializerClass the serializer class name.
     * @return the builder
     */
    public JetStreamSinkBuilder<InputT> payloadSerializerClass(String payloadSerializerClass) {
        return super.payloadSerializerClass(payloadSerializerClass);
    }

    /**
     * Set sink properties from a properties object
     * See the readme and {@link Constants} for property keys
     * @param properties the properties object
     * @return the builder
     */
    public JetStreamSinkBuilder<InputT> sinkProperties(Properties properties) {
        return properties(properties);
    }

    /**
     * Build a NatsSink. Subject and
     * @return the sink
     */
    public JetStreamSink<InputT> build() {
        beforeBuild();
        return new JetStreamSink<>(subjects, payloadSerializer, connectionFactory);
    }
}
