// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.examples;

import io.nats.client.ConsumeOptions;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import io.synadia.flink.payload.StringPayloadDeserializer;
import io.synadia.flink.source.JetStreamSource;
import io.synadia.flink.source.JetStreamSourceBuilder;
import io.synadia.flink.source.JetStreamSubjectConfiguration;
import io.synadia.flink.utils.PropertiesUtils;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Properties;

import static io.synadia.flink.utils.Constants.JETSTREAM_SUBJECT_CONFIGURATIONS;
import static io.synadia.flink.utils.Constants.PAYLOAD_DESERIALIZER;
import static io.synadia.flink.utils.MiscUtils.getClassName;

public abstract class ConfigurationFileGenerator {

    public static void main(String[] args) throws IOException {
        JetStreamSubjectConfiguration configA = JetStreamSubjectConfiguration.builder()
            .streamName("StreamA")
            .startSequence(42L)
            .maxMessagesToRead(100)
            .ack(true)
            .buildWithSubject("SubjectA");

        JetStreamSubjectConfiguration configB = JetStreamSubjectConfiguration.builder()
            .streamName("StreamB")
            .startTime(ZonedDateTime.now().minusHours(24))
            .buildWithSubject("SubjectB");

        List<JetStreamSubjectConfiguration> configsC = JetStreamSubjectConfiguration.builder()
            .streamName("StreamC")
            .consumeOptions(ConsumeOptions.builder()
                .batchSize(100)
                .build())
            .buildWithSubjects("SubjectC1", "SubjectC2");

        Properties connectionProps = PropertiesUtils.loadPropertiesFromFile("src/examples/resources/connection.properties");

        JetStreamSource<String> sourceA = new JetStreamSourceBuilder<String>()
            .connectionProperties(connectionProps)
            .payloadDeserializer(new StringPayloadDeserializer())
            .addSubjectConfigurations(configA)
            .build();

        JetStreamSource<String> sourceBC = new JetStreamSourceBuilder<String>()
            .connectionProperties(connectionProps)
            .payloadDeserializer(new StringPayloadDeserializer())
            .addSubjectConfigurations(configB)
            .addSubjectConfigurations(configsC)
            .build();

        JsonUtils.printFormatted(getConfiguration(sourceA));

        System.out.println("-----");
        JsonUtils.printFormatted(getConfiguration(sourceBC));
    }

    public static JsonValue getConfiguration(JetStreamSource<?> source) {
        JsonValueUtils.MapBuilder bm = JsonValueUtils.mapBuilder();
        JsonValueUtils.ArrayBuilder ba = JsonValueUtils.arrayBuilder();
        for (String id : source.configById.keySet()) {
            ba.add(source.configById.get(id).toJsonValue());
        }
        bm.put(PAYLOAD_DESERIALIZER, getClassName(source.payloadDeserializer));
        bm.put(JETSTREAM_SUBJECT_CONFIGURATIONS, ba.jv);
        return bm.jv;
    }
}
