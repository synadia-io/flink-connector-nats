// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.BaseConsumeOptions;
import io.nats.client.support.DateTimeUtils;
import io.synadia.flink.TestBase;
import io.synadia.flink.message.AsciiStringSourceConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static io.synadia.flink.utils.Constants.UTF8_STRING_SOURCE_CONVERTER_CLASSNAME;
import static org.junit.jupiter.api.Assertions.*;

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

        List<JetStreamSubjectConfiguration> subjectConfigurations = new ArrayList<>();
        subjectConfigurations.add(JetStreamSubjectConfiguration.builder()
            .streamName("StreamA")
            .subject("SubjectA")
            .maxMessagesToRead(100)
            .startSequence(101L)
            .build());
        subjectConfigurations.add(JetStreamSubjectConfiguration.builder()
            .streamName("StreamB")
            .subject("SubjectB")
            .maxMessagesToRead(50)
            .startSequence(51L)
            .build());
        subjectConfigurations.add(null); // COVERAGE

        if (which.equals("MaxMessages")) {
            JetStreamSource<String> manualSource = new JetStreamSourceBuilder<String>()
                .connectionPropertiesFile(TEST_CONNECTION_PROPERTIES_FILE)
                .sourceConverterClass(UTF8_STRING_SOURCE_CONVERTER_CLASSNAME)
                .addSubjectConfigurations((List<JetStreamSubjectConfiguration>)null) // COVERAGE
                .addSubjectConfigurations(subjectConfigurations) // COVERAGE
                .setSubjectConfigurations(subjectConfigurations)
                .build();
            validateSourceFileConstruction(jsonSource, manualSource);
        }
        else if (which.equals("Endless")) {
            JetStreamSource<String> manualSource = new JetStreamSourceBuilder<String>()
                .connectionPropertiesFile(TEST_CONNECTION_PROPERTIES_FILE)
                .sourceConverter(new AsciiStringSourceConverter()) // COVERAGE
                .sourceConverterClass(UTF8_STRING_SOURCE_CONVERTER_CLASSNAME)
                .addSubjectConfigurations((JetStreamSubjectConfiguration[]) null) // COVERAGE
                .setSubjectConfigurations((JetStreamSubjectConfiguration[]) null) // COVERAGE
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
                        .ackBehavior(AckBehavior.NoAck)
                        .build(),
                    JetStreamSubjectConfiguration.builder()
                        .streamName("StreamMany")
                        .subject("Subject1")
                        .build(),
                    JetStreamSubjectConfiguration.builder()
                        .streamName("StreamMany")
                        .subject("Subject2")
                        .build(),
                    null // COVERAGE
                )
                .build();
            validateSourceFileConstruction(jsonSource, manualSource);
        }
    }

    @Test
    void testQueueCapacity_overflowsIntRangeThrows() {
        // A single subject at Integer.MAX_VALUE batchSize contributes
        // batchSize + max(1, batchSize * 25 / 100) ≈ 2.68 billion in long math,
        // which exceeds Integer.MAX_VALUE — the range check after the
        // accumulator should fire instead of silently wrapping to a negative.
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> new JetStreamSourceBuilder<String>()
                .connectionPropertiesFile(TEST_CONNECTION_PROPERTIES_FILE)
                .sourceConverter(new AsciiStringSourceConverter())
                .addSubjectConfigurations(JetStreamSubjectConfiguration.builder()
                    .streamName("S").subject("huge").batchSize(Integer.MAX_VALUE).build())
                .build());
        assertTrue(iae.getMessage().contains("exceeds Integer.MAX_VALUE"), iae.getMessage());
    }

    private static void validateSourceFileConstruction(JetStreamSource<String> expected, JetStreamSource<String> actual) throws Exception {
        assertEquals(expected.config, actual.config);
        assertEquals(expected.config.boundedness, actual.getBoundedness());
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

        // \/ lots of just coverage from here on \/
        assertEquals(TypeInformation.of(String.class), actual.getProducedType());

        String s = actual.toString();
        assertTrue(s.contains("JetStreamSource"));
        assertTrue(s.contains("Utf8StringSourceConverter"));

        assertEquals(expected, actual);
        //noinspection EqualsWithItself,SimplifiableAssertion
        assertTrue(actual.equals(actual));
        //noinspection SimplifiableAssertion
        assertFalse(actual.equals(new Object()));

        assertEquals(expected.hashCode(), actual.hashCode());
        assertNotNull(actual.getSplitSerializer());
        assertNotNull(actual.getEnumeratorCheckpointSerializer());

        assertNotNull(actual.createReader(new SourceReaderContext() {
            @Override
            public SourceReaderMetricGroup metricGroup() {
                return null;
            }

            @Override
            public Configuration getConfiguration() {
                return null;
            }

            @Override
            public String getLocalHostName() {
                return "";
            }

            @Override
            public int getIndexOfSubtask() {
                return 0;
            }

            @Override
            public void sendSplitRequest() {
            }

            @Override
            public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return null;
            }
        }));
    }

    @Test
    void testQueueCapacity_sumsBatchPlusRePullPerSubject() throws Exception {
        // Two subjects at jnats defaults (batchSize=500, threshold=25%):
        // per-subject contribution is 500 + max(1, 500*25/100) = 500 + 125 = 625.
        int defaultPer = BaseConsumeOptions.DEFAULT_MESSAGE_COUNT
            + Math.max(1, BaseConsumeOptions.DEFAULT_MESSAGE_COUNT
                * BaseConsumeOptions.DEFAULT_THRESHOLD_PERCENT / 100);

        JetStreamSource<String> source = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(TEST_CONNECTION_PROPERTIES_FILE)
            .sourceConverter(new AsciiStringSourceConverter())
            .addSubjectConfigurations(
                JetStreamSubjectConfiguration.builder().streamName("S").subject("a").build(),
                JetStreamSubjectConfiguration.builder().streamName("S").subject("b").build())
            .build();
        assertEquals(2 * defaultPer, source.config.sourceQueueCapacity);
    }

    @Test
    void testQueueCapacity_rePullFloorsAtOne() throws Exception {
        // batchSize=1 with default threshold yields rePull = max(1, 0) = 1.
        JetStreamSource<String> source = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(TEST_CONNECTION_PROPERTIES_FILE)
            .sourceConverter(new AsciiStringSourceConverter())
            .addSubjectConfigurations(JetStreamSubjectConfiguration.builder()
                .streamName("S").subject("one").batchSize(1).build())
            .build();
        assertEquals(2, source.config.sourceQueueCapacity);
    }

    @Test
    void testConstructionErrors() {
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> new JetStreamSourceBuilder<String>()
                .build());
        assertTrue(iae.getMessage().contains("Valid source converter class must be provided."));

        iae = assertThrows(IllegalArgumentException.class,
            () -> new JetStreamSourceBuilder<String>()
                .sourceConverter(new AsciiStringSourceConverter())
                .build());
        assertTrue(iae.getMessage().contains("At least 1 managed subject configuration is required."));

        iae = assertThrows(IllegalArgumentException.class,
            () -> new JetStreamSourceBuilder<String>()
                .sourceConverter(new AsciiStringSourceConverter())
                .addSubjectConfigurations(
                    JetStreamSubjectConfiguration.builder()
                        .streamName("Un")
                        .subject("UnSub")
                        .build(),
                    JetStreamSubjectConfiguration.builder()
                        .streamName("Bound")
                        .subject("BoundSub")
                        .maxMessagesToRead(1)
                        .build())
                .build());
        assertTrue(iae.getMessage().contains("All boundedness must be the same."));
   }
}