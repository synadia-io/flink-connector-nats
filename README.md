![Synadia](src/main/javadoc/images/synadia-logo.png)

![NATS](src/main/javadoc/images/nats-logo.png)

# Synadia's Flink to NATS Java Connector

Connect NATS to Flink with Java

**Current Release**: 2.0.0-beta4 &nbsp; **Current Snapshot**: 2.0.0-beta5-SNAPSHOT

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/flink-connector-nats/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/flink-connector-nats)
[![javadoc](https://javadoc.io/badge2/io.synadia/flink-connector-nats/javadoc.svg)](https://javadoc.io/doc/io.synadia/flink-connector-nats)
[![Coverage Status](https://coveralls.io/repos/github/synadia-io/flink-connector-nats/badge.svg?branch=main)](https://coveralls.io/github/synadia-io/flink-connector-nats?branch=main)
[![Build Main Badge](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-main.yml/badge.svg?event=push)](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-main.yml)
[![Release Badge](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-release.yml/badge.svg?event=release)](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-release.yml)

## Java Version

The connector requires Java version 11 or later to be compatible with Flink libraries. 
The JNATS library is built with Java 8 and is compatible being run by a later version of Java.  

## Beta Release and Implementation Versioning

This library adheres to [semver](https://semver.org/). 
In order to allow for completely different implementations for JetStream support,
the implementations have been put into distinct Java packages, for instance
`io.synadia.flink.v0` This has allowed us to release this while provide the future ability 
to _change_ the api without requiring a semver major bump. To accomplish this, the
new implementation will be put in the `io.synadia.flink.v1` package hierarchy.

The reason the current release is also _beta_ is that we are hoping for use and feedback. 

## Source
In order to construct a source, you must use the builder.
* The NatsSourceBuilder is generic. It's generic type, `<OutputT>` is the type of object that will be created from a 
  message's subject, headers and payload data byte[]
* You must set or include properties to construct a connection unless you are connecting to 'nats://localhost:4222' with no security.
* The builder has these methods:
    ```
    subjects(String... subjects)
    subjects(List<String> subjects)
    connectionProperties(Properties connectionProperties)
    connectionPropertiesFile(String connectionPropertiesFile)
    minConnectionJitter(long minConnectionJitter)
    maxConnectionJitter(long maxConnectionJitter)
    payloadDeserializer(PayloadDeserializer<InputT> payloadDeserializer)
    payloadDeserializerClass(String payloadDeserializerClass)
    sourceProperties(Properties properties)
    ```
* When using the builder, the last call value is used, they are not additive.
  * Calling multiple variations or instances of `subjects`
  * Calling `connectionProperties` or `connectionPropertiesFile`
  * Calling `payloadSerializer` or `payloadSerializerClass`
  * Calling `sourceProperties`

* You can supply source settings with code
  ```java
  NatsSource<String> source = new NatsSourceBuilder<String>()
      .subjects("subject1", "subject1")
      .connectionPropertiesFile("/path/to/jnats_client_connection.properties")
      .minConnectionJitter(1000)
      .maxConnectionJitter(5000)
      .payloadDeserializer("io.synadia.payload.StringPayloadDeserializer")
      .build();
  ```
  
* You can also supply source properties from a file.

  ```java
  Properties props = Utils.loadPropertiesFromFile("/path/to/source.properties");
  NatsSource<String> source = new NatsSourceBuilder<String>()
      .sourceProperties(props)
      .connectionPropertiesFile("/path/to/jnats_client_connection.properties")
      .build();
  ```

* The source supports these property keys
  ```properties
  source.subjects
  source.payload.deserializer
  source.startup.jitter.min
  source.startup.jitter.max
  ```

* It's okay to use the combine the source properties and the connection properties

  ```java
  Properties props = Utils.loadPropertiesFromFile("/path/to/sourceAndConnection.properties");
  NatsSource<String> source = new NatsSourceBuilder<String>()
      .sourceProperties(props)
      .connectionProperties(props)
      .build();
  ```

  -or-

  ```java
  Properties props = Utils.loadPropertiesFromFile("/path/to/sourceAndConnection.properties");
  NatsSource<String> source = new NatsSourceBuilder<String>()
      .sourceProperties(props)
      .connectionPropertiesFile("/path/to/sourceAndConnection.properties")
      .build();
  ```

## Sink

In order to construct a sink, you must use the builder.
* The NatsSinkBuilder is generic. It's generic type, `<InputT>` is the type of object you expect from a source that will become the byte[] payload of a message.
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
    sinkProperties(Properties properties)
    ```
  
* When using the builder, the last call value is used, they are not additive.
  * Calling multiple variations or instances of `subjects`
  * Calling `connectionProperties` or `connectionPropertiesFile`
  * Calling `payloadSerializer` or `payloadSerializerClass`
  * Calling `sinkProperties`

* You can supply sink settings with code

  ```java
  NatsSink<String> sink = new NatsSinkBuilder<String>()
      .subjects("subject1", "subject2")
      .connectionPropertiesFile("/path/to/jnats_client_connection.properties")
      .minConnectionJitter(1000)
      .maxConnectionJitter(5000)
      .payloadSerializerClass("io.synadia.payload.StringPayloadSerializer")
      .build();
  ```

* You can also supply sink properties from a file.

  ```java
  Properties props = Utils.loadPropertiesFromFile("/path/to/sink.properties");
  NatsSink<String> sink = new NatsSinkBuilder<String>()
      .sinkProperties(props)
      .connectionPropertiesFile("/path/to/jnats_client_connection.properties")
      .build();
  ```

* The sink supports these property keys
  ```properties
  sink.subjects
  sink.payload.serializer
  sink.startup.jitter.min
  sink.startup.jitter.max
  ```

* It's okay to use the combine the sink properties and the connection properties

  ```java
  Properties props = Utils.loadPropertiesFromFile("/path/to/sinkAndConnection.properties");
  NatsSink<String> sink = new NatsSinkBuilder<String>()
      .sinkProperties(props)
      .connectionProperties(props)
      .build();
  ```
  -or-

  ```java
  Properties props = Utils.loadPropertiesFromFile("/path/to/sinkAndConnection.properties");
  NatsSink<String> sink = new NatsSinkBuilder<String>()
      .sinkProperties(props)
      .connectionPropertiesFile("/path/to/sinkAndConnection.properties")
      .build();
  ```

## Connection Properties

If it was not clear from the source and sink sections, 
there are two ways to get the connection properties into the sink or source:

1\. by passing a file location via `.connectionPropertiesFile(String)`

When the sink or source are given a properties file location, 
this must be an existing path on every instance of Flink in the cluster environment, 
otherwise it will not be found and the sink or source won't be able to connect.

  ```java
  NatsSink<String> sink = new NatsSinkBuilder<String>()
      ...
      .connectionPropertiesFile("/path/to/jnats_client_connection.properties")
      ...        
      .build();
  ```

2\. by passing a Properties object prepared in code `.connectionProperties(Properties)`

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

#### Connection Properties Reference
For full connection properties see the [NATS - Java Client, README Options Properties](https://github.com/nats-io/nats.java#options---properties)

#### Example Connection Properties File

```properties
io.nats.client.keyStorePassword=kspassword
io.nats.client.keyStore=/path/to/keystore.jks
io.nats.client.trustStorePassword=tspassword
io.nats.client.trustStore=/path/to/truststore.jks
io.nats.client.url=tls://myhost:4222
```

## Serializers / Deserializers

There are 3 ways to get a de/serializer into your source/sink.

1\. By giving the fully qualified class name.
  ```java
  NatsSink<String> sink = new NatsSinkBuilder<String>
      ...
      .payloadSerializerClass("io.synadia.flink.payload.StringPayloadSerializer")
      ...
      .build();
  ```

  ```java
  NatsSource<String> source = new NatsSourceBuilder<String>
      ...
      .payloadDeserializerClass("io.synadia.flink.payload.StringPayloadDeserializer")
      ...
      .build();
  ```

2\. By supplying an instance of the serializer
  ```java
  StringPayloadSerializer serializer = new StringPayloadSerializer();
  NatsSink<String> sink = new NatsSinkBuilder<String>
      ...
      .payloadSerializer(serializer)
      ...
      .build();
  ```

  ```java
  StringPayloadDeserializer serializer = new StringPayloadDeserializer();
  NatsSource<String> source = new NatsSourceBuilder<String>
      ...
      .payloadDeserializer(serializer)
      ...
      .build();
  ```

3\. By supplying the fully qualified name as a property 
  ```properties
  sink.payload.serializer=com.mycompany.MySerializer
  ```
  ```java
  Properties props = Utils.loadPropertiesFromFile("/path/to/sink.properties");
  NatsSink<MySerializerType> sink = new NatsSinkBuilder<MySerializerType>
      ...
      .sinkProperties(props)
      ...
      .build();
  ```

  ```properties
  source.payload.deserializer=com.mycompany.MyDeserializer
  ```
  ```java
  Properties props = Utils.loadPropertiesFromFile("/path/to/source.properties");
  NatsSource<MyDeserializerType> source = new NatsSourceBuilder<MyDeserializerType>
      ...
      .sourceProperties(props)
      ...
      .build();
  ```

There is nothing significantly different, it is simply a developer's preference. In either case the class
* must have a no parameter constructor
* must be of the proper input or output type for the sink or source
* must `implements Serializable`, which is usually trivial since there is should not be any state.
  If any of these conditions are not met, a terminal exception will be thrown.

## Deployed Library

### Using Gradle

The NATS client is available in the Maven central repository, and can be imported as a standard dependency in your `build.gradle` file:

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

### Using Maven

The NATS client is available on the Maven central repository, and can be imported as a normal dependency in your pom.xml file:

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

Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
