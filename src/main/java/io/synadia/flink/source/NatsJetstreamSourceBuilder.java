// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.support.SerializableConsumerConfiguration;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.config.SourceConfiguration;

public class NatsJetstreamSourceBuilder<OutputT> {

    private PayloadDeserializer<OutputT> deserializationSchema;

    private String natsUrl;

    private SerializableConsumerConfiguration serializableConsumerConfiguration;

    private String subject;

    public NatsJetstreamSourceBuilder setDeserializationSchema(PayloadDeserializer<OutputT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return  this;
    }

    public NatsJetstreamSourceBuilder setNatsUrl(String natsUrl) {
        this.natsUrl = natsUrl;
        return this;
    }

    public NatsJetstreamSourceBuilder setCc(SerializableConsumerConfiguration serializableConsumerConfiguration) {
        this.serializableConsumerConfiguration = serializableConsumerConfiguration;
        return this;
    }

    public NatsJetstreamSourceBuilder setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    /**
     * Build a NatsSource.
     * @return the source
     */
    public NatsJetStreamSource<OutputT> build() {
        if (deserializationSchema == null) {
            throw new IllegalStateException("Valid payload serializer class must be provided.");
        }
        if (serializableConsumerConfiguration == null ) {
            throw new IllegalStateException("Consumer configuration not provided");
        }
        SourceConfiguration
                sourceConfiguration = new SourceConfiguration(subject, natsUrl, serializableConsumerConfiguration);

        return new NatsJetStreamSource<>(deserializationSchema, sourceConfiguration);
    }
}
