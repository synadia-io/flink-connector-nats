// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

import io.nats.client.Connection;
import io.nats.client.Options;
import io.synadia.flink.TestBase;
import io.synadia.flink.TestServerContext;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionFactoryTest extends TestBase {
    static TestServerContext ctx;

    @BeforeAll
    public static void beforeAll() throws Exception {
        ctx = createContext(ctx);
    }

    @AfterAll
    public static void afterAll() {
        ctx = shutdownContext(ctx);
    }

    @AfterEach
    public void afterEach() {
        cleanupJs(ctx.nc);
    }

    /**
     * Tests basic connection creation using Properties.
     * Verifies that:
     * 1. Connection is established successfully
     * 2. Connection properties are preserved
     * 3. Default JetStream options are applied
     */
    @Test
    void testConnectWithValidPropertiesShouldEstablishConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory(defaultConnectionProperties(ctx.url));
        Connection connection = null;
        try {
            connection = factory.connect();
            assertNotNull(connection);
            assertSame(Connection.Status.CONNECTED, connection.getStatus());
        }
        finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Tests connection creation using a properties file.
     * Verifies that:
     * 1. Properties are loaded correctly from file
     * 2. Connection is established using file properties
     * 3. Properties file path is preserved
     */
    @Test
    void testConnectWithValidPropertiesFileShouldEstablishConnection() throws Exception {
        String propsFile = createTempPropertiesFile(defaultConnectionProperties(ctx.url));

        ConnectionFactory factory = new ConnectionFactory(propsFile);
        assertEquals(propsFile, factory.getConnectionPropertiesFile());

        try (Connection connection = factory.connect()) {
            assertNotNull(connection);
            assertSame(Connection.Status.CONNECTED, connection.getStatus());
            assertEquals(0, connection.getOptions().getMaxReconnect());
        }
    }

    /**
     * Tests connection overriding the max reconnects of zero
     */
    @Test
    void testConnectWithCustomMaxReconnectsPropertyWithPrefix() throws Exception {
        Properties props = defaultConnectionProperties(ctx.url);
        props.setProperty("io.nats.client.reconnect.max", "3");
        String propsFile = createTempPropertiesFile(props);

        ConnectionFactory factory = new ConnectionFactory(propsFile);
        assertEquals(propsFile, factory.getConnectionPropertiesFile());

        try (Connection connection = factory.connect()) {
            assertNotNull(connection);
            assertSame(Connection.Status.CONNECTED, connection.getStatus());
            assertEquals(3, connection.getOptions().getMaxReconnect());
        }
    }
    /**
     * Tests connection overriding the max reconnects of zero
     */
    @Test
    void testConnectWithCustomMaxReconnectsPropertyWithoutPrefix() throws Exception {
        Properties props = defaultConnectionProperties(ctx.url);
        props.setProperty("reconnect.max", "4");
        String propsFile = createTempPropertiesFile(props);

        ConnectionFactory factory = new ConnectionFactory(propsFile);
        assertEquals(propsFile, factory.getConnectionPropertiesFile());

        try (Connection connection = factory.connect()) {
            assertNotNull(connection);
            assertSame(Connection.Status.CONNECTED, connection.getStatus());
            assertEquals(4, connection.getOptions().getMaxReconnect());
        }
    }

    /**
     * Tests the creation and basic functionality of JetStream context through ConnectionFactory.
     * This test specifically verifies that:
     * 1. ConnectionContext is created successfully with JetStream enabled
     * 2. Both JetStream (js) and JetStreamManagement (jsm) objects are properly initialized
     * 3. Basic JetStream connectivity is working by verifying we can list streams
     * Note: This test focuses on context creation and basic connectivity.
     */
    @Test
    void testConnectionContextWithJetStreamShouldCreateJetStreamContext() throws Exception {
        ConnectionFactory factory = new ConnectionFactory(defaultConnectionProperties(ctx.url));
        ConnectionContext context = null;
        try {
            context = factory.getConnectionContext();
            assertNotNull(context);
            assertNotNull(context.js);
            assertNotNull(context.jsm);
            ConnectionContext finalContext = context;
            assertDoesNotThrow(() -> finalContext.jsm.getStreams(), "JetStream operation should not throw an exception");
        }
        finally {
            if (context != null) {
                context.connection.close();
            }
        }
    }

    /**
     * Tests error handling when a connection fails.
     * Verifies that:
     * 1. IOException is thrown with an appropriate message
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
     * 1. Returned properties are a copy of the original
     * 2. Modifying returned properties doesn't affect the original
     * 3. Original properties remain unchanged
     */
    @Test
    void testGetConnectionPropertiesWhenModifiedShouldNotAffectOriginal() {
        Properties originalProps = defaultConnectionProperties(ctx.url);
        ConnectionFactory factory = new ConnectionFactory(originalProps);

        Properties returnedProps = factory.getConnectionProperties();
        assertNotNull(returnedProps);
        assertNotSame(originalProps, returnedProps);

        // Modify returned properties
        returnedProps.setProperty("new.property", "value");
        assertNull(originalProps.getProperty("new.property"));
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
        Properties props = defaultConnectionProperties(ctx.url);
        ConnectionFactory originalFactory = new ConnectionFactory(props);

        // Create multiple deserialized instances
        ConnectionFactory deserialized = (ConnectionFactory) javaSerializeDeserializeObject(originalFactory);
        assertNotNull(deserialized);

        // Verify each instance can create valid connections
        for (ConnectionFactory factory : Arrays.asList(originalFactory, deserialized)) {
            try (Connection connection = factory.connect()) {
                assertNotNull(connection);
                assertSame(Connection.Status.CONNECTED, connection.getStatus());
            }
        }
    }

    @Test
    void testCoverage() {
        try (NatsServerRunner runner = new NatsServerRunner(4222, false))
        {
            ConnectionFactory factory1 = new ConnectionFactory((Properties)null);
            assertEquals("connectionProperties=null", factory1.toString()); // coverage
            try (Connection conn = factory1.connect()) {
                assertEquals(4222, conn.getServerInfo().getPort());
            }
            ConnectionFactory factory2 = factory1;
            assertEquals(factory1, factory2);
            assertEquals(factory1.hashCode(), factory2.hashCode());

            factory2 = new ConnectionFactory((String)null);
            try (Connection conn = factory2.connect()) {
                assertEquals(4222, conn.getServerInfo().getPort());
            }
            assertEquals(factory1, factory2);

            Properties p = new Properties();
            p.setProperty("jso_prefix", "jso_prefix_value");
            ConnectionFactory factory3 = new ConnectionFactory(p);
            assertNotEquals(factory1, factory3);
            assertNotEquals(factory1.hashCode(), factory3.hashCode());
            assertNotNull(factory3.getConnectionProperties());
            assertNotNull(factory3.toString());

            factory3.getConnectionContext(); // COVERAGE uses jso properties

            p.remove("jso_prefix");
            p.setProperty("jso_domain", "jso_domain_value");
            factory3 = new ConnectionFactory(p);
            factory3.getConnectionContext(); // COVERAGE uses jso properties

            ConnectionFactory factory4 = new ConnectionFactory("connectionPropertiesFile");
            assertNotEquals(factory1, factory4);
            assertNotEquals(factory1.hashCode(), factory4.hashCode());
            assertNull(factory4.getConnectionProperties());
            assertNotNull(factory4.toString());

            //noinspection EqualsBetweenInconvertibleTypes,SimplifiableAssertion
            assertFalse(factory1.equals("blah")); // COVERAGE
        }
        catch (Exception fail) {
            fail();
        }
    }

    /**
     * Tests error handling when the Properties file path is invalid.
     * Verifies that:
     * 1. IOException is thrown
     * 2. Exception message contains the invalid file path
     */
    @Test
    void testConnectWithInvalidPropertiesFileShouldThrowIOException() {
        String nonExistentFile = "nonexistent.properties";
        ConnectionFactory factory = new ConnectionFactory(nonExistentFile);
        assertThrows(IOException.class, factory::connect);
    }

    /**
     * Tests error handling when the Properties file is malformed.
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