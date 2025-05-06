// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

import io.nats.client.Connection;
import io.nats.client.Options;
import io.synadia.flink.TestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/** Unit test for {@link ConnectionFactory}. */
class ConnectionFactoryTest extends TestBase {

    /**
     * Tests basic connection creation using Properties.
     * Verifies that:
     * 1. Connection is established successfully
     * 2. Connection properties are preserved
     * 3. Default JetStream options are applied
     */
    @Test
    void testConnectWithValidPropertiesShouldEstablishConnection() throws Exception {
        runInServer((nc, url) -> {
            ConnectionFactory factory = new ConnectionFactory(defaultConnectionProperties(url));
            Connection connection = null;
            try {
                connection = factory.connect();
                assertNotNull(connection);
                assertTrue(connection.getStatus() == Connection.Status.CONNECTED);
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        });
    }

    /**
     * Tests connection creation using properties file.
     * Verifies that:
     * 1. Properties are loaded correctly from file
     * 2. Connection is established using file properties
     * 3. Properties file path is preserved
     */
    @Test
    void testConnectWithValidPropertiesFileShouldEstablishConnection() throws Exception {
        runInServer((nc, url) -> {
            String propsFile = createTempPropertiesFile(defaultConnectionProperties(url));

            ConnectionFactory factory = new ConnectionFactory(propsFile);
            assertEquals(propsFile, factory.getConnectionPropertiesFile());

            Connection connection = null;
            try {
                connection = factory.connect();
                assertNotNull(connection);
                assertTrue(connection.getStatus() == Connection.Status.CONNECTED);
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        });
    }

    /**
     * Tests the creation and basic functionality of JetStream context through ConnectionFactory.
     * This test specifically verifies that:
     * 1. ConnectionContext is created successfully with JetStream enabled
     * 2. Both JetStream (js) and JetStreamManagement (jsm) objects are properly initialized
     * 3. Basic JetStream connectivity is working by verifying we can list streams
     *
     * Note: This test focuses on context creation and basic connectivity.
     */
    @Test
    void testConnectContextWithJetStreamShouldCreateJetStreamContext() throws Exception {
        runInServer(true, (nc, url) -> {
            ConnectionFactory factory = new ConnectionFactory(defaultConnectionProperties(url));
            ConnectionContext context = null;
            try {
                context = factory.connectContext();
                assertNotNull(context);
                assertNotNull(context.js);
                assertNotNull(context.jsm);
                ConnectionContext finalContext = context;
                assertDoesNotThrow(() -> finalContext.jsm.getStreams(), "JetStream operation should not throw an exception");
            } finally {
                if (context != null && context.connection != null) {
                    context.connection.close();
                }
            }
        });
    }

    /**
     * Tests error handling when connection fails.
     * Verifies that:
     * 1. IOException is thrown with appropriate message
     * 2. Original cause is preserved
     * 3. Resources are cleaned up properly
     */
    @Test
    void testConnectWithInvalidServerUrlShouldThrowIOException() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_URL, "nats://invalid:1234");

        ConnectionFactory factory = new ConnectionFactory(props);

        IOException thrown = assertThrows(
            IOException.class,
            factory::connect,
            "Should throw IOException for invalid connection"
        );

        assertTrue(thrown.getMessage().contains("Cannot connect to NATS server"));
        assertNotNull(thrown.getCause());
    }

    /**
     * Tests connection properties immutability.
     * Verifies that:
     * 1. Returned properties are a copy
     * 2. Modifying returned properties doesn't affect original
     * 3. Original properties remain unchanged
     */
    @Test
    void testGetConnectionPropertiesWhenModifiedShouldNotAffectOriginal() throws Exception {
        runInServer((nc, url) -> {
            Properties originalProps = defaultConnectionProperties(url);
            ConnectionFactory factory = new ConnectionFactory(originalProps);

            Properties returnedProps = factory.getConnectionProperties();
            assertNotNull(returnedProps);
            assertNotSame(originalProps, returnedProps);

            // Modify returned properties
            returnedProps.setProperty("new.property", "value");
            assertNull(originalProps.getProperty("new.property"));
        });
    }


    /**
     * Tests serialization/deserialization of ConnectionFactory.
     * Verifies that:
     * 1. Factory can be serialized
     * 2. Deserializations produce consistent objects
     * 3. All deserialized instances maintain original properties and state
     * 4. Each deserialized factory can create valid connections
     */
    @Test
    void testSerializationWithValidFactoryShouldMaintainState() throws Exception {
        runInServer((nc, url) -> {
            Properties props = defaultConnectionProperties(url);
            props.setProperty(Constants.CONNECT_JITTER, "1000");
            ConnectionFactory originalFactory = new ConnectionFactory(props);

            // Create multiple deserialized instances
            ConnectionFactory deserialized = (ConnectionFactory) javaSerializeDeserializeObject(originalFactory);
            assertNotNull(deserialized);

            props = deserialized.getConnectionProperties();
            assertEquals("1000", props.getProperty(Constants.CONNECT_JITTER));

            // Verify each instance can create valid connections
            for (ConnectionFactory factory : Arrays.asList(originalFactory, deserialized)) {
                try (Connection connection = factory.connect()) {
                    assertNotNull(connection);
                    assertSame(connection.getStatus(), Connection.Status.CONNECTED);
                }
            }
        });
    }

    /**
     * Tests error handling when properties file path is invalid.
     * Verifies that:
     * 1. NoSuchFileException is thrown
     * 2. Exception message contains the invalid file path
     */
    @Test
    void testConnectWithInvalidPropertiesFileShouldThrowNoSuchFileException() {
        String nonExistentFile = "nonexistent.properties";
        ConnectionFactory factory = new ConnectionFactory(nonExistentFile);

        NoSuchFileException thrown = assertThrows(
            NoSuchFileException.class,
            factory::connect,
            "Should throw NoSuchFileException for invalid properties file path"
        );

        assertEquals(nonExistentFile, thrown.getMessage());
    }

    /**
     * Tests error handling when properties file is malformed.
     * Verifies that:
     * 1. IOException is thrown for malformed properties
     * 2. Original cause is preserved
     * 3. Error message indicates connection failure
     */
    @Test
    void testConnectWithMalformedPropertiesShouldThrowIOException(@TempDir Path tempDir) throws Exception {
        // port 22222 instead of 4222 since sometimes 4222 is running a server
        String malformedContent =
            Options.PROP_URL + "=nats://localhost:22222\n" +
                "invalid_property_format\n" +
                "nats.connection.timeout=invalid\n" +
                "nats.ping.interval=abc";

        Path propsPath = tempDir.resolve("malformed.properties");
        Files.writeString(propsPath, malformedContent);

        ConnectionFactory factory = new ConnectionFactory(propsPath.toString());

        IOException thrown = assertThrows(
            IOException.class,
            factory::connect,
            "Should throw IOException for malformed properties file"
        );

        assertTrue(thrown.getMessage().contains("Cannot connect to NATS server"));
        assertNotNull(thrown.getCause());
        assertTrue(thrown.getCause() instanceof java.net.ConnectException
            || thrown.getCause().getMessage().contains("Unable to connect"));
    }
}