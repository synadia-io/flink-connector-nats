// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.synadia.flink.TestBase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for {@link JetStreamSourceBuilder}. */
class JetStreamSourceBuilderTest extends TestBase {
   @Test
    void testConstruction() throws Exception {

      JetStreamSource<String> jsonSource = new JetStreamSourceBuilder<String>()
          .connectionPropertiesFile("src/test/resources/connection.properties")
          .sourceJson("src/test/resources/sourceA.json")
          .build();

      JetStreamSource<String> yamlSource = new JetStreamSourceBuilder<String>()
          .connectionPropertiesFile("src/test/resources/connection.properties")
          .sourceYaml("src/test/resources/sourceA.yaml")
          .build();

      assertEquals(jsonSource.boundedness, yamlSource.boundedness);
      assertEquals(jsonSource.configById, yamlSource.configById);
      assertEquals(jsonSource.payloadDeserializer.getClass(), yamlSource.payloadDeserializer.getClass());
      assertEquals(jsonSource.connectionFactory, yamlSource.connectionFactory);

      jsonSource = new JetStreamSourceBuilder<String>()
          .connectionPropertiesFile("src/test/resources/connection.properties")
          .sourceJson("src/test/resources/sourceBC.json")
          .build();

      yamlSource = new JetStreamSourceBuilder<String>()
          .connectionPropertiesFile("src/test/resources/connection.properties")
          .sourceYaml("src/test/resources/sourceBC.yaml")
          .build();

      assertEquals(jsonSource.boundedness, yamlSource.boundedness);
      assertEquals(jsonSource.configById, yamlSource.configById);
      assertEquals(jsonSource.payloadDeserializer.getClass(), yamlSource.payloadDeserializer.getClass());
      assertEquals(jsonSource.connectionFactory, yamlSource.connectionFactory);
    }
}