// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.synadia.flink.TestBase;
import io.synadia.flink.payload.StringPayloadDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static io.nats.client.support.ApiConstants.SUBJECTS;
import static io.synadia.flink.utils.PropertyConstants.PAYLOAD_DESERIALIZER;
import static io.synadia.flink.utils.PropertyConstants.STRING_PAYLOAD_DESERIALIZER_CLASSNAME;
import static org.junit.jupiter.api.Assertions.*;

/** Unit test for {@link NatsSourceBuilder}. */
class NatsSourceBuilderTest extends TestBase {

    /**
     * Tests the minimum configuration required to successfully build a NatsSource.
     * This represents the most basic use case of the builder.
     *
     * Required settings:
     * 1. At least one subject to subscribe to
     * 2. A payload deserializer to convert NATS messages
     * 3. Connection properties for NATS server
     *
     * Example usage:
     * ```java
     * NatsSource<String> source = new NatsSourceBuilder<String>()
     *     .subjects("orders.>")  // Subscribe to all messages in 'orders' hierarchy
     *     .payloadDeserializer(new StringPayloadDeserializer())
     *     .connectionProperties(props)  // props containing "nats.connection.url", etc.
     *     .build();
     * ```
     */
    @Test
    void testBuildWithMinimumRequiredSettings() throws Exception {
        runInServer((nc, url) -> {
            String subject = subject();

            NatsSource<String> source = new NatsSourceBuilder<String>()
                .subject(subject)
                .payloadDeserializer(new StringPayloadDeserializer())
                .connectionPropertiesFile(defaultConnectionProperties(url))
                .build();

            assertNotNull(source, "Built source should not be null");
        });
    }

    /**
     * Tests the builder's ability to configure a source using Properties object.
     * Demonstrates how to configure the source using properties file approach,
     * which is useful for externalized configuration.
     * Properties tested:
     * 1. Connection settings (URL, credentials, etc.)
     * 2. Source-specific settings (subjects, deserializer)
     * Example properties file:
     * ```properties
     * # Connection settings
     * nats.connection.url=nats://localhost:4222
     * nats.connection.max.reconnects=5
     * # Source settings
     * nats.source.subjects=orders.>,payments.>
     * nats.source.payload.deserializer=io.synadia.flink.payload.StringPayloadDeserializer
     * ```
     * Usage:
     * ```java
     * Properties props = loadProperties("config.properties");
     * NatsSource<String> source = new NatsSourceBuilder<String>()
     *     .sourceProperties(props)
     *     .connectionProperties(props)
     *     .build();
     * ```
     */
    @Test
    void testSourceProperties_WithValidConfiguration() throws Exception {
        runInServer((nc, url) -> {
            Properties props = defaultConnectionProperties(url);
            // Add source specific properties
            String subject = subject();
            props.setProperty(SUBJECTS, subject);
            props.setProperty(PAYLOAD_DESERIALIZER, STRING_PAYLOAD_DESERIALIZER_CLASSNAME);

            String propsFile = writeToTempFile("tsp", props);

            NatsSource<String> source = new NatsSourceBuilder<String>()
                .connectionPropertiesFile(props)
                .sourceProperties(propsFile)
                .build();

            assertNotNull(source);
            assertEquals(1, source.subjects.size());
            assertEquals(subject, source.subjects.get(0));
        });
    }

    /**
     * Tests the builder's validation logic for various invalid inputs.
     * Ensures the builder fails fast with clear error messages when
     * misconfigured.
     *
     * Validation scenarios:
     * 1. Empty subjects array - Should fail as at least one subject is required
     *    ```java
     *    .subjects(new String[0])  // Should throw IllegalStateException
     *    ```
     *
     * 2. Null subjects - Should fail with clear error message
     *    ```java
     *    .subjects((String[])null)  // Should throw IllegalStateException
     *    ```
     *
     * 3. Missing connection properties - Should fail as connection details are required
     *    ```java
     *    .subjects("test")
     *    .payloadDeserializer(deserializer)
     *    .build()  // Should throw IllegalStateException - missing connection props
     *    ```
     *
     * Expected: IllegalStateException with descriptive message for each case
     */
    @Test
    void testBuild_WithInvalidInputs() throws Exception {
        runInServer((nc, url) -> {
            Properties props = defaultConnectionProperties(url);

            // Test empty subjects
            IllegalArgumentException emptySubjectsEx = assertThrows(
                IllegalArgumentException.class,
                () -> new NatsSourceBuilder<String>()
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .connectionPropertiesFile(defaultConnectionProperties(url))
                    .build()
            );

            // Test null subjects
            IllegalArgumentException nullSubjectsEx = assertThrows(
                IllegalArgumentException.class,
                () -> new NatsSourceBuilder<String>()
                    .subject((String[])null)
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .connectionPropertiesFile(props)
                    .build()
            );

            // Test missing required properties
            IllegalArgumentException missingPropsEx = assertThrows(
                IllegalArgumentException.class,
                () -> new NatsSourceBuilder<String>()
                    .subject(subject())
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .build()  // Missing connection properties
            );
        });
    }

    /**
     * Tests the builder's fluent interface implementation.
     * Verifies that the builder maintains proper object state through method chaining
     * and returns consistent builder instances.
     *
     * Aspects tested:
     * 1. Method chaining - Each builder method returns the same builder instance
     * 2. State consistency - Final build reflects all chained configurations
     *
     * Example of fluent interface usage:
     * ```java
     * NatsSourceBuilder<String> builder = new NatsSourceBuilder<>();
     * NatsSource<String> source = builder
     *     .subjects("orders.>")
     *     .payloadDeserializer(new StringPayloadDeserializer())
     *     .connectionProperties(props)
     *     .build();
     *
     * // Both references should point to same builder instance
     * assertSame(builder, builder.subjects("test"));
     * ```
     */
    @Test
    void testBuilderMethods_ReturnSameInstance() throws Exception {
        runInServer((nc, url) -> {
            String subject = subject();
            Properties props = defaultConnectionProperties(url);

            NatsSourceBuilder<String> builder = new NatsSourceBuilder<String>();

            // Test that each method returns the same builder instance
            assertSame(builder, builder.subject(subject),
                "subjects() should return same builder instance");
            assertSame(builder, builder.payloadDeserializer(new StringPayloadDeserializer()),
                "payloadDeserializer() should return same builder instance");
            assertSame(builder, builder.connectionPropertiesFile(props),
                "connectionProperties() should return same builder instance");

            // Test that chained configuration works
            NatsSource<String> source = builder.build();
            assertNotNull(source, "Source built with chained methods should not be null");
        });
    }

    /**
     * Tests the builder's support for class name-based deserializer configuration.
     * This approach allows for dynamic loading of deserializers without direct class dependencies.
     *
     * Use cases:
     * 1. Configuration-driven deserializer selection
     * 2. Plugin-style deserializer loading
     *
     * Example configurations:
     * ```java
     * // Using class name string
     * .payloadDeserializerClass("io.synadia.flink.payload.StringPayloadDeserializer")
     *
     * // Or via properties
     * props.setProperty("nats.source.payload.deserializer",
     *     "io.synadia.flink.payload.StringPayloadDeserializer");
     * ```
     *
     * The builder should:
     * 1. Load the specified class
     * 2. Verify it implements PayloadDeserializer
     * 3. Instantiate it using the default constructor
     */
    @Test
    void testBuild_WithPayloadDeserializerClass() throws Exception {
        runInServer((nc, url) -> {
            String subject = subject();

            NatsSource<String> source = new NatsSourceBuilder<String>()
                .subject(subject)
                .payloadDeserializerClass(STRING_PAYLOAD_DESERIALIZER_CLASSNAME)
                .connectionPropertiesFile(defaultConnectionProperties(url))
                .build();

            assertNotNull(source, "Source with deserializer class should not be null");
        });
    }

    /**
     * Tests the builder's support for multiple subject subscriptions.
     * Demonstrates how to configure a source to receive messages from multiple NATS subjects.
     *
     * Common use cases:
     * 1. Aggregating data from multiple topics
     * 2. Pattern-based subject subscriptions
     * 3. Multi-tenant message processing
     *
     * Example configurations:
     * ```java
     * // Direct subject list
     * .subjects("orders.usa.>", "orders.eu.>", "orders.asia.>")
     *
     * // Via properties
     * props.setProperty("nats.source.subjects", "orders.usa.>,orders.eu.>,orders.asia.>")
     *
     * // Using wildcards
     * .subjects("orders.*.completed", "payments.*.processed")
     * ```
     *
     * The source should:
     * 1. Subscribe to all specified subjects
     * 2. Handle messages from any of the subjects
     * 3. Properly distribute messages to Flink
     */
    @Test
    void testBuild_WithMultipleSubjects() throws Exception {
        runInServer((nc, url) -> {
            String subject1 = subject();
            String subject2 = subject();
            String subject3 = subject();

            NatsSource<String> source = new NatsSourceBuilder<String>()
                .subject(subject1, subject2, subject3)
                .payloadDeserializer(new StringPayloadDeserializer())
                .connectionPropertiesFile(defaultConnectionProperties(url))
                .build();

            assertNotNull(source, "Source with multiple subjects should not be null");
        });
    }
}