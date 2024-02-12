// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.synadia.flink.Utils;
import io.synadia.flink.common.NatsSinkOrSourceBuilder;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;

import java.util.List;
import java.util.Properties;

import static io.synadia.flink.Constants.*;

public class NatsJetStreamSourceBuilder<OutputT> extends NatsSinkOrSourceBuilder<NatsJetStreamSourceBuilder<OutputT>> {

    private DeserializationSchema<OutputT> deserializationSchema;
    private NatsConsumeOptions natsConsumeOptions;
    private Boundedness mode = Boundedness.BOUNDED; //default

    @Override
    protected NatsJetStreamSourceBuilder<OutputT> getThis() {
        return this;
    }

    /**
     * Set the deserializer for the source.
     * @param deserializationSchema the deserializer.
     * @return the builder
     */
    public NatsJetStreamSourceBuilder<OutputT> payloadDeserializer(DeserializationSchema<OutputT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    /**
     * Set source properties from a properties object
     * See the readme and {@link io.synadia.flink.Constants} for property keys
     * @param properties the properties object
     * @return the builder
     */
    public NatsJetStreamSourceBuilder<OutputT> sourceProperties(Properties properties) {
        List<String> subjects = Utils.getPropertyAsList(properties, SOURCE_SUBJECTS);
        if (!subjects.isEmpty()) {
            subjects(subjects);
        }

        long l = Utils.getLongProperty(properties, SOURCE_STARTUP_JITTER_MIN, -1);
        if (l != -1) {
            minConnectionJitter(l);
        }

        l = Utils.getLongProperty(properties, SOURCE_STARTUP_JITTER_MAX, -1);
        if (l != -1) {
            maxConnectionJitter(l);
        }

        return this;
    }

    public NatsJetStreamSourceBuilder<OutputT> consumerConfig(NatsConsumeOptions config) {
        this.natsConsumeOptions = config;
        return this;
    }

    public NatsJetStreamSourceBuilder<OutputT> boundedness(Boundedness mode) {
        this.mode = mode;
        return this;
    }

    /**
     * Build a NatsSource. Subject and
     * @return the source
     */
    public NatsJetStreamSource<OutputT> build() {
        beforeBuild();
        if (deserializationSchema == null) {
            throw new IllegalStateException("Valid payload serializer class must be provided.");
        }
        return new NatsJetStreamSource<>(deserializationSchema, createConnectionFactory(), subjects.get(0), natsConsumeOptions, mode);
    }
}
