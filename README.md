![Synadia](src/main/javadoc/images/synadia-logo.png)

![NATS](src/main/javadoc/images/nats-logo.png)

# Synadia's Flink to NATS Java Connector

Connect NATS to Flink with Java

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/flink-connector-nats/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/flink-connector-nats)
[![javadoc](https://javadoc.io/badge2/io.synadia/flink-connector-nats/javadoc.svg)](https://javadoc.io/doc/io.synadia/flink-connector-nats)
[![Coverage Status](https://coveralls.io/repos/github/synadia-io/flink-connector-nats/badge.svg?branch=main)](https://coveralls.io/github/synadia-io/flink-connector-nats?branch=main)
[![Build Main Badge](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-main.yml/badge.svg?event=push)](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-main.yml)
[![Release Badge](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-release.yml/badge.svg?event=release)](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-release.yml)

## Sink
In order to construct a sink, you must use the builder. 
* The NatsSinkBuilder is generic. It's generic type, &lt;InputT&gt; is the type of object you expect from a source that will become the byte[] payload of a message.
* You must set or include properties to construct a connection unless you are connecting to 'nats://localhost:4222' with no security. 
* The builder has these methods:
    ```
    subjects(String... subjects)
    subjects(List<String> subjects)
    connectionProperties(Properties connectionProperties)
    connectionPropertiesFile(String connectionPropertiesFile)
    minConnectionJitter(long minConnectionJitter)
    maxConnectionJitter(long maxConnectionJitter)
    payloadSerializer(PayloadSerializer<InputT> payloadSerializer)
    payloadSerializerClass(String payloadSerializerClass)
    ```
* When using the builder, the last call value is used, they are not additive.
  * Calling multiple variations or instances of `subjects`
  * Calling `properties` or `propertiesFile`
  * Calling `payloadSerializer` or `payloadSerializerClass`

```java
NatsSink<String> sink = NatsSink.<String>builder
    .subjects("sink1", "sink2")
    .connectionPropertiesFile("/path/to/jnats_client_connection.properties")
    .minConnectionJitter(1000)
    .maxConnectionJitter(5000)
    .payloadSerializerClass("com.mycompany.StringPayloadSerializer")
    .build();
```

# Source
In order to construct a source, you must use the builder.
* The NatsSourceBuilder is generic. It's generic type, &lt;OutputT&gt; is the type of object you will provide to a sink that the byte[] payload of a message.
* You must set or include properties to construct a connection unless you are connecting to 'nats://localhost:4222' with no security.
* The builder has these methods:
    ```
    subjects(String... subjects)
    subjects(List<String> subjects)
    connectionProperties(Properties connectionProperties)
    connectionPropertiesFile(String connectionPropertiesFile)
    minConnectionJitter(long minConnectionJitter)
    maxConnectionJitter(long maxConnectionJitter)
    payloadDeserializer(PayloadDeserializer<OutputT> payloadDeserializer)
    payloadDeserializerClass(String payloadDeserializerClass)
    ```
* When using the builder, the last call value is used, they are not additive.
  * Calling multiple variations or instances of `subjects`
  * Calling `properties` or `propertiesFile`
  * Calling `payloadSerializer` or `payloadSerializerClass`

```java
NatsSource<String> source = NatsSource.<String>builder
    .subjects("source1", "source2")
    .connectionPropertiesFile("/path/to/jnats_client_connection.properties")
    .minConnectionJitter(1000)
    .maxConnectionJitter(5000)
    .payloadSerializerClass("com.mycompany.StringPayloadDeserializer")
    .build();
```

# Example Properties File

For full connection properties see the [NATS - Java Client, README Options Properties](https://github.com/nats-io/nats.java#options---properties)

```properties
io.nats.client.keyStorePassword=kspassword
io.nats.client.keyStore=/path/to/keystore.jks
io.nats.client.trustStorePassword=tspassword
io.nats.client.trustStore=/path/to/truststore.jks
io.nats.client.url=tls://myhost:4222
```