![Synadia](src/main/javadoc/images/synadia-logo.png)

![NATS](src/main/javadoc/images/nats-logo.png)

# Synadia's Flink to NATS Java Connector

Connect NATS to Flink with Java

**Current Release**: 2.0.0-beta4 &nbsp; **Current Snapshot**: 2.1.0-rc1-SNAPSHOT

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/flink-connector-nats/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/flink-connector-nats)
[![javadoc](https://javadoc.io/badge2/io.synadia/flink-connector-nats/javadoc.svg)](https://javadoc.io/doc/io.synadia/flink-connector-nats)
[![Coverage Status](https://coveralls.io/repos/github/synadia-io/flink-connector-nats/badge.svg?branch=main)](https://coveralls.io/github/synadia-io/flink-connector-nats?branch=main)
[![Build Main Badge](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-main.yml/badge.svg?event=push)](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-main.yml)
[![Release Badge](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-release.yml/badge.svg?event=release)](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-release.yml)

## Table of Contents
* [Java Version](#java-version)
* [Builders](#builders) 
* [Nats Message Processing](#nats-message-processing)
* [Sources](#sources)
  * [NatsSource](#natssource)
  * [JetStreamSource](#jetstreamsource)
* [Sinks](#sinks)
  * [NatsSink](#natssink)
  * [JetStreamSink](#jetstreamsink)
* [Source and Sink Configuration](#source-and-sink-configuration)
  * [Connection Properties](#connection-properties)
  * [Serializers / Deserializers](#serializers--deserializers)
* [Getting the Library](#getting-the-library)
  * [Gradle](#gradle)
  * [Maven](#maven)
* [License](#license)
 
## Java Version

The connector requires Java version 11 or later to be compatible with Flink libraries. 
The JNATS library is built with Java 8 and is compatible with being run by a later version of Java.  

## Beta Release and Implementation Versioning

The current release is a _beta_ and all APIs are subject to change. 

## Builders

There are fluent builders throughout the code. 

1\. The only way to supply connection information is through a Properties object instance or a Properties file. 
All connections are built using the properties that the JNats client Options object accepts. 
If no connection properties are supplied, the configuration will assume the default insecure url `nats://localhost:4222`.
More details can be found in the [Connection Properties](#connection-properties) section of this document.

2\. You can provide a lot of configuration directly from configuration files.
But if a configuration property can be set from a property _and_ can be set directly with a fluent builder,
the last one called will be used. For example
* There is a property `sink_converter` where you can specify the `sinkConverterClass`
but if you call `sourceProperties(...)` with properties that has that key, 
and then you call `sinkConverterClass(...)` the value set directly will be used.

3\. The JetStreamSourceBuilder has both "set" and "add" methods. 
* If you `setSubjectConfigurations(...)` you replace whatever was in place before with the new configuration(s).
* If you `addSubjectConfigurations(...)` you append the new configuration(s) to whatever was already set or added. 

4\. You can supply source and sink settings with code, for example...
  ```java
  NatsSource<String> source = new NatsSourceBuilder<String>()
      .connectionPropertiesFile("/path/to/jnats_client_connection.properties")
      .sourceConverter("io.synadia.message.StringMessageReader")
      .subjects("subject1", "subject1")
      .build();
  ```

5\. For `NatsSource`, `NatsSink` and `JetStreamSink` you can supply properties from either a Properties, YAML or JSON file.

  ```java
  NatsSource<String> source = new NatsSourceBuilder<String>()
        .connectionPropertiesFile("/path/to/connection.properties")
        .sourceProperties("/path/to/source.yaml")
        .build();
  ```

  ```java
  NatsSink<String> sink = new NatsSinkBuilder<String>()
        .connectionPropertiesFile("/path/to/connection.properties")
        .sinkYaml("/path/to/source.yaml")
        .build();
  ```

  ```java
  JetStreamSink<String> sink = new JetStreamSinkBuilder<String>()
      .connectionPropertiesFile("/path/to/connection.properties")
      .sinkJson("/path/to/json.yaml")
      .build();
  ```

For `JetStreamSource` you can only use YAML or JSON due to the complexity for the configuration. 

## Connection Properties

There are two ways to get the connection properties into the sink or source:

1\. Passing a file location via `.connectionPropertiesFile(String)`

When the sink or source are given a properties file location,
this must be an existing path on every instance of Flink in the cluster environment.
Otherwise, it will not be found, and the sink or source won't be able to connect.
This is probably the most likely way that you will configure the connection.

  ```java
  NatsSink<String> sink = new NatsSinkBuilder<String>()
      ...
      .connectionPropertiesFile("/path/to/connection.properties")
      ...        
      .build();
  ```

2\. Passing a Properties object prepared in code `.connectionProperties(Properties)`

When the properties are prepared in code and given as an object, it is serialized during construction
so it can be sent over the wire to nodes that will be running instances of sink or source.
This will reduce the requirement for having a properties file on each instance but at the tradeoff
of having this information passed over the wire in normal java object serialization form, which is not
necessarily secure.

  ```java
  Properties connectionProperties = ...
  NatsSink<String> sink = new NatsSinkBuilder<String>
      ...
      .connectionProperties(connectionProperties)
      ...        
      .build();
  ```

### Connection Properties Reference
For full connection properties see the [NATS - Java Client, README Options Properties](https://github.com/nats-io/nats.java?tab=readme-ov-file#properties)

### Example Connection Properties File

This is a simple example [Connection Properties File](src/examples/resources/connection.properties).

This is an example of using properties to set up tls:
```properties
io.nats.client.url=tls://myhost:4222
io.nats.client.keyStorePassword=kspassword
io.nats.client.keyStore=/path/to/keystore.jks
io.nats.client.trustStorePassword=tspassword
io.nats.client.trustStore=/path/to/truststore.jks
```

## Source and Sink Concepts

This project's sources get messages from NATS and emit them in a form that can be received by a sink.
This is done via payload deserializer and sink converters.

A deserializer is used by the source to take data from the origin it knows about, 
in our case a NATS message received while subscribing to a NATS subject.
It then converts it into a format that the sink understands and emits it. 
The output of a source becomes the input of a sink.

A serializer is used by the sink to take data it got from the source and output it to its origin, 
in our case publishing to a NATS subject.

There will probably be a time when you read data from a source other than NATS or write data to a source
other than NATS. It will be up to you to understand what a foreign source outputs or a foreign sink expects
as input. 

### Foreign Source to a NATS or JetStream Sink
Maybe a foreign source emits an encrypted array of bytes. If you are just storing that data as is as the payload of 
a NATS message, you can use the build-in `ByteArrayMessageReader`. Otherwise, using that class as your starting
point, you can customize what it does with that encrypted input. This project's sinks expect a byte[] to be used
as the message data published to the configured sink subjects, which is why all sink converters used for
this project's sinks output a byte[].

### NATS or JetStream Source to a Foreign Sink.
Maybe you want to sink a subject or stream of data to some foreign sink. Maybe that foreign sink is just a passthrough
for message data in either byte or String form, then you can use the provided sink converters.
But maybe you have headers in your messages, and you combine those headers, the subject and the into some JSON,
then you will provide a custom MessageReader. A deserializer receives the entire NATS `Message`
so you can extract all the information you need. If you are using the JetStreamSource, the message will actually 
be a NatsJetStreamMessage, so the metadata, including the stream sequence, will be available.

### Built In Implementations

The project's sources require an implementation of:

```java
public interface MessageReader<OutputT> extends Serializable, ResultTypeQueryable<OutputT> {
    /**
     * Convert a Message into an instance of the output type.
     * @return the output object
     */
    OutputT getObject(Message message);
}
```

The built-in payload deserializers are:

* `StringMessageReader` takes a NATS Message and converts the message data byte array to a String
* `ByteArrayMessageReader` takes a NATS Message and converts the message data byte array (`byte[]`) and converts it to the object form (`Byte[]`)

The project's sinks require an implementation of:

```java
public interface MessageReader<InputT> extends Serializable {
  /**
   * Get bytes from the input object so they can be published in a message
   * @param input the input object
   * @return the bytes
   */
  byte[] getBytes(InputT input);
}
```

The built-in sink converters are:

* `StringMessageReader` takes a takes a String and converts it to a byte array
* `ByteArrayMessageSupplier` A ByteArrayMessageSupplier takes a byte array in object form (`Byte[]`) and converts it to a byte array (`byte[]`).

## Sources

There are two types of Flink source implementations available. 
1. NatsSource subscribes one or more core NATS subjects.
2. JetStreamSource subscribes one or more JetStream subjects.

### NatsSource

A NatsSource subscribes to one or more core NATS subjects and uses a MessageReader implementation to convert the 
message to the output type emitted to sinks. To construct a NatsSource, you must use the NatsSourceBuilder.

* Each subject is subscribed in its own split, which Flink can run in different threads or in different nodes depending
on your Flink configuration.

* A NatsSource is currently only unbounded, meaning it runs forever. 
It's on the TODO list to make this the source able to be bounded. 

The source can be configured in code or with the Properties, JSON or YAML. It supports these property keys:
* `source_converter_class_name`
* `subjects`

Here are examples for 
  [Properties](src/examples/resources/core-source-config.properties),
  [JSON](src/examples/resources/core-source-config.json) and
  [YAML](src/examples/resources/core-source-config.yaml)

## NatsSink

NatsSink expects some sort of data, most likely either a String or a byte[].
If turns around and uses that data for the data in a NATS Message  
and publishes that same data to all subjects configured for the sink. It's basically just a core NATS publisher.

If for instance you want to publish 

To construct a sink, you must use the builder.
* The NatsSinkBuilder is generic. Its generic type, `<InputT>` is the type of object you expect from a source that will become the byte[] payload of a message.
* You must set or include properties to construct a connection unless you are connecting to 'nats://localhost:4222' with no security.
* the builder has these methods:
    ```
    subjects(String... subjects)
    subjects(List<String> subjects)
    connectionProperties(Properties connectionProperties)
    connectionPropertiesFile(String connectionPropertiesFile)
    sinkConverter(SinkMessageSupplier<InputT> sinkConverter)
    sinkConverterClass(String sinkConverterClass)
    sinkProperties(Properties properties)
    ```
  
* When using the builder, the last call value is used; they are not additive.
  * Calling multiple variations or instances of `subjects`
  * Calling `connectionProperties` or `connectionPropertiesFile`
  * Calling `sinkConverter` or `sinkConverterClass`
  * Calling `sinkProperties`

* You can supply sink settings with code

  ```java
  NatsSink<String> sink = new NatsSinkBuilder<String>()
      .subjects("subject1", "subject2")
      .connectionPropertiesFile("/path/to/connection.properties")
      .sinkConverterClass("io.synadia.message.StringMessageSupplier")
      .build();
  ```

* You can also supply sink properties from a file.

  ```java
  Properties props = Utils.loadPropertiesFromFile("/path/to/sink.properties");
  NatsSink<String> sink = new NatsSinkBuilder<String>()
      .sinkProperties(props)
      .connectionPropertiesFile("/path/to/connection.properties")
      .build();
  ```

* The sink supports these property keys
  ```properties
  subjects
  sink_converter_class_name
  ```

### Serializers / Deserializers

There are three ways to configure a serializer / deserializer into your source/sink.

1\. By giving the fully qualified class name.
  ```java
  NatsSink<String> sink = new NatsSinkBuilder<String>
      ...
      .sinkConverterClass("io.synadia.flink.message.Utf8StringSinkConverter")
      .build();

  NatsSource<String> source = new NatsSourceBuilder<String>
      ...
      .sourceConverterClass("io.synadia.flink.message.Utf8StringSourceConverter")
      .build();
  ```

2\. By supplying an instance of the serializer
  ```java
  StringMessageSupplier serializer = new StringMessageSupplier();
  NatsSink<String> sink = new NatsSinkBuilder<String>
      ...
      .sinkConverter(serializer)
      .build();
  
  StringMessageReader serializer = new StringMessageReader();
  NatsSource<String> source = new NatsSourceBuilder<String>
      ...
      .sourceConverter(serializer)
      .build();
  ```

3\. By supplying the fully qualified name as a property 
  ```properties
  sink_converter_class_name=com.mycompany.MySupplier
  source_converter_class_name=com.mycompany.MyDeserializer
  ```

  ```java
  Properties props = Utils.loadPropertiesFromFile("/path/to/my.properties");
  NatsSink<MySerializerType> sink = new NatsSinkBuilder<MySerializerType>
      .sinkProperties(props)
      ...
      .build();
  
  NatsSource<MyDeserializerType> source = new NatsSourceBuilder<MyDeserializerType>
      .sourceProperties(props)
      ...
      .build();
  ```

There is nothing significantly different, it is simply a developer's preference. In either case the class
* must have a no parameter constructor
* must be of the proper input or output type for the sink or source
* must `implements Serializable`, which is usually trivial since there is should not be any state.

If any of these conditions are not met, a terminal exception will be thrown.

## Getting the Library

The NATS client is available in the Maven central repository, and can be imported as a standard dependency in your `build.gradle` file:

### Gradle

```groovy
dependencies {
    implementation 'io.synadia:flink-connector-nats:{major.minor.patch}'
}
```

If you need the latest and greatest before Maven central updates, you can use:

```groovy
repositories {
    mavenCentral()
    maven {
        url "https://s01.oss.sonatype.org/content/repositories/releases"
    }
}
```

If you need a snapshot version, you must add the url for the snapshots and change your dependency.

```groovy
repositories {
    mavenCentral()
    maven {
        url "https://s01.oss.sonatype.org/content/repositories/snapshots"
    }
}

dependencies {
   implementation 'io.synadia:flink-connector-nats:{major.minor.patch}-SNAPSHOT'
}
```

### Maven

```xml
<dependency>
    <groupId>io.synadia</groupId>
    <artifactId>flink-connector-nats</artifactId>
    <version>{major.minor.patch}</version>
</dependency>
```

If you need the absolute latest, before it propagates to maven central, you can use the repository:

```xml
<repositories>
    <repository>
        <id>sonatype releases</id>
        <url>https://s01.oss.sonatype.org/content/repositories/releases</url>
        <releases>
           <enabled>true</enabled>
        </releases>
    </repository>
</repositories>
```

If you need a snapshot version, you must enable snapshots and change your dependency.

```xml
<repositories>
    <repository>
        <id>sonatype snapshots</id>
        <url>https://s01.oss.sonatype.org/content/repositories/snapshots</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>

<dependency>
    <groupId>io.synadia</groupId>
    <artifactId>flink-connector-nats</artifactId>
    <version>{major.minor.patch}-SNAPSHOT</version>
</dependency>
```
## License

Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
