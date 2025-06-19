// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.support.DateTimeUtils;
import io.synadia.flink.TestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.synadia.flink.utils.Constants.UTF8_STRING_SOURCE_CONVERTER_CLASSNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for {@link JetStreamSourceBuilder}. */
class JetStreamSourceBuilderTest extends TestBase {

    @ParameterizedTest
    @ValueSource(strings = {"MaxMessages", "Endless"})
    void testConstruction(String which) throws Exception {
        String jsonSourceFile = "src/test/resources/source" + which + ".json";
        String yamlSourceFile = "src/test/resources/source" + which + ".yaml";
        JetStreamSource<String> jsonSource = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(TEST_CONNECTION_PROPERTIES_FILE)
            .jsonConfigFile(jsonSourceFile)
            .build();

        JetStreamSource<String> yamlSource = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(TEST_CONNECTION_PROPERTIES_FILE)
            .yamlConfigFile(yamlSourceFile)
            .build();
        validateSourceFileConstruction(jsonSource, yamlSource);

        String jsonFile = writeToTempFile("JetStreamSource", ".json)", yamlSource.toJson());
        String yamlFile = writeToTempFile("JetStreamSource", ".yaml)", yamlSource.toYaml());

        JetStreamSource<String> jsonSource2 = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(TEST_CONNECTION_PROPERTIES_FILE)
            .jsonConfigFile(jsonFile)
            .build();
        validateSourceFileConstruction(jsonSource, jsonSource2);

        JetStreamSource<String> yamlSource2 = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(TEST_CONNECTION_PROPERTIES_FILE)
            .yamlConfigFile(yamlFile)
            .build();
        validateSourceFileConstruction(jsonSource, yamlSource2);

        if (which.equals("MaxMessages")) {
            JetStreamSource<String> manualSource = new JetStreamSourceBuilder<String>()
                .connectionPropertiesFile(TEST_CONNECTION_PROPERTIES_FILE)
                .sourceConverterClass(UTF8_STRING_SOURCE_CONVERTER_CLASSNAME)
                .addSubjectConfigurations(
                    JetStreamSubjectConfiguration.builder()
                        .streamName("StreamA")
                        .subject("SubjectA")
                        .maxMessagesToRead(100)
                        .startSequence(101L)
                        .build(),
                    JetStreamSubjectConfiguration.builder()
                        .streamName("StreamB")
                        .subject("SubjectB")
                        .maxMessagesToRead(50)
                        .startSequence(51L)
                        .build()
                )
                .build();
            validateSourceFileConstruction(jsonSource, manualSource);
        }
        else if (which.equals("Endless")) {
            JetStreamSource<String> manualSource = new JetStreamSourceBuilder<String>()
                .connectionPropertiesFile(TEST_CONNECTION_PROPERTIES_FILE)
                .sourceConverterClass(UTF8_STRING_SOURCE_CONVERTER_CLASSNAME)
                .addSubjectConfigurations(
                    JetStreamSubjectConfiguration.builder()
                        .streamName("StreamSS")
                        .subject("SubjectSS")
                        .startSequence(42L)
                        .build(),
                    JetStreamSubjectConfiguration.builder()
                        .streamName("StreamST")
                        .subject("SubjectST")
                        .startTime(DateTimeUtils.parseDateTime("2025-04-08T00:38:32.109526400Z"))
                        .build(),
                    JetStreamSubjectConfiguration.builder()
                        .streamName("StreamBS")
                        .subject("SubjectBS")
                        .batchSize(100)
                        .build(),
                    JetStreamSubjectConfiguration.builder()
                        .streamName("StreamTP")
                        .subject("SubjectTP")
                        .thresholdPercent(75)
                        .build(),
                    JetStreamSubjectConfiguration.builder()
                        .streamName("StreamAM")
                        .subject("SubjectAM")
                        .ackPolicy()
                        .build(),
                    JetStreamSubjectConfiguration.builder()
                        .streamName("StreamMany")
                        .subject("Subject1")
                        .build(),
                    JetStreamSubjectConfiguration.builder()
                        .streamName("StreamMany")
                        .subject("Subject2")
                        .build()
                )
                .build();
            validateSourceFileConstruction(jsonSource, manualSource);
        }
    }

    private static void validateSourceFileConstruction(JetStreamSource<String> expected, JetStreamSource<String> actual) {
        assertEquals(expected.boundedness, actual.boundedness);
        assertEquals(expected.configById.size(), actual.configById.size());
        for (String id : expected.configById.keySet()) {
            JetStreamSubjectConfiguration expectedConfig = expected.configById.get(id);
            JetStreamSubjectConfiguration actualConfig = actual.configById.get(id);
            assertEquals(expectedConfig, actualConfig);
        }
        assertEquals(expected.configById, actual.configById);
        assertEquals(expected.sourceConverter.getClass(), actual.sourceConverter.getClass());
        assertEquals(expected.connectionFactory, actual.connectionFactory);
        assertEquals(expected, actual);
    }
}