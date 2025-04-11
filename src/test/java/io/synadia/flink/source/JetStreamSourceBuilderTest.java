// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.synadia.flink.TestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.nats.client.support.JsonUtils.printFormatted;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for {@link JetStreamSourceBuilder}. */
class JetStreamSourceBuilderTest extends TestBase {

    public static final String CONNECTION_PROPERTIES_FILE = "src/test/resources/connection.properties";

    @ParameterizedTest
    @ValueSource(strings = {"sourceA", "sourceBC"})
    void testConstruction(String which) throws Exception {
        String jsonSourceFile = "src/test/resources/" + which + ".json";
        String yamlSourceFile = "src/test/resources/" + which + ".yaml";
        JetStreamSource<String> jsonSource = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(CONNECTION_PROPERTIES_FILE)
            .sourceJson(jsonSourceFile)
            .build();

        JetStreamSource<String> yamlSource = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(CONNECTION_PROPERTIES_FILE)
            .sourceYaml(yamlSourceFile)
            .build();

        printFormatted(yamlSource.toJson());
        System.out.println(yamlSource.toYaml());

        String jsonFile = writeToTempFile("JetStreamSource", ".json)", yamlSource.toJson());
        String yamlFile = writeToTempFile("JetStreamSource", ".yaml)", yamlSource.toYaml());

        JetStreamSource<String> jsonSource2 = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(CONNECTION_PROPERTIES_FILE)
            .sourceJson(jsonFile)
            .build();

        JetStreamSource<String> yamlSource2 = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(CONNECTION_PROPERTIES_FILE)
            .sourceYaml(yamlFile)
            .build();

        validate(jsonSource, yamlSource);
        validate(jsonSource, jsonSource2);
        validate(jsonSource, yamlSource2);
    }

    private static void validate(JetStreamSource<String> expected, JetStreamSource<String> actual) {
        assertEquals(expected.boundedness, actual.boundedness);
        assertEquals(expected.configById, actual.configById);
        assertEquals(expected.payloadDeserializer.getClass(), actual.payloadDeserializer.getClass());
        assertEquals(expected.connectionFactory, actual.connectionFactory);
        assertEquals(expected, actual);
    }
}