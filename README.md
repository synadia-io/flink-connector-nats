![Synadia](src/main/javadoc/images/synadia-logo.png)

![NATS](src/main/javadoc/images/nats-logo.png)

# Synadia's Flink to NATS Java Connector

Connect NATS to Flink with Java

**Current Release**: N/A &nbsp; **Current Snapshot**: 0.0.1-SNAPSHOT

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/flink-connector-nats/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/flink-connector-nats)
[![javadoc](https://javadoc.io/badge2/io.synadia/flink-connector-nats/javadoc.svg)](https://javadoc.io/doc/io.synadia/flink-connector-nats)
[![Coverage Status](https://coveralls.io/repos/github/synadia-io/flink-connector-nats/badge.svg?branch=main)](https://coveralls.io/github/synadia-io/flink-connector-nats?branch=main)
[![Build Main Badge](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-main.yml/badge.svg?event=push)](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-main.yml)
[![Release Badge](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-release.yml/badge.svg?event=release)](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-release.yml)

## Java Version

The connector requires Java version 11 or later to be compatible with Flink libraries. 
The JNATS library is built with Java 8 and is compatible being run by a later version of Java.  

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
      .payloadSerializerClass("io.synadia.payload.StringPayloadSerializer")
      .build();
  ```

## Connection Properties

There are 2 ways to get the connection properties into the sink or source:

1\. by passing a file location via `.connectionPropertiesFile(String)`

When the sink or source are given a properties file location, 
this must be an existing path on every instance of Flink in the cluster environment, 
otherwise it will not be found and the sink or source won't be able to connect.

  ```java
  NatsSink<String> sink = NatsSink.<String>builder
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
  NatsSink<String> sink = NatsSink.<String>builder
      ...
      .connectionProperties(connectionProperties)
      ...        
      .build();
  ```

#### Connection Properties Reference
For full connection properties see the [NATS - Java Client, README Options Properties](https://github.com/nats-io/nats.java#options---properties)

#### Example Properties File

```properties
io.nats.client.keyStorePassword=kspassword
io.nats.client.keyStore=/path/to/keystore.jks
io.nats.client.trustStorePassword=tspassword
io.nats.client.trustStore=/path/to/truststore.jks
io.nats.client.url=tls://myhost:4222
```

## Serializers / Deserializers

There are 2 ways to get a serializer into your sink and the parallel for getting a desrializer into your source.

1\. By giving the fully qualified class name.
  ```java
  NatsSink<String> sink = NatsSink.<String>builder
      ...
      .payloadSerializerClass("io.synadia.payload.StringPayloadSerializer")
      ...
      .build();
  ```

2\. By supplying an instance of the serializer
  ```java
  StringPayloadSerializer serializer = new StringPayloadSerializer();
  NatsSink<String> sink = NatsSink.<String>builder
      ...
      .payloadSerializer(serializer)
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
