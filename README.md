![Synadia](src/main/javadoc/images/synadia-logo.png)

# Synadia NATS to Flink Connector

**Current Release**: 2.1.1 &nbsp; **Current Snapshot**: 2.1.2-SNAPSHOT

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/flink-connector-nats/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/flink-connector-nats)
[![javadoc](https://javadoc.io/badge2/io.synadia/flink-connector-nats/javadoc.svg)](https://javadoc.io/doc/io.synadia/flink-connector-nats)
[![Build Main Badge](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-main.yml/badge.svg?event=push)](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-main.yml)
[![Release Badge](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-release.yml/badge.svg?event=release)](https://github.com/synadia-io/flink-connector-nats/actions/workflows/build-release.yml)

## Table of Contents
* [Java Version](#java-version)
* [Releases and Versioning](#releases-and-versioning)
* [Builders](#builders)
* [Connection Properties](#connection-properties)
* [Source and Sink Concepts](#source-and-sink-concepts)
* [Converters](#converters) 
* [Sinks](#sinks)
* [Sources](#sources)
* [Getting the Library](#getting-the-library)
* [License](#license)
 
## Java Version

The connector requires Java version 11 or later to be compatible with Flink libraries. 
The JNats library is built with Java 8 and is compatible with being run by a later version of Java.  

## Releases and Versioning

This project will adhere to semver except where a class is commented as "INTERNAL" and annotated with the `@Internal` annotation
(`org.apache.flink.annotation.Internal`) 

## Builders

There are fluent builders throughout the code. 

1\. The only way to supply connection information is through a Properties object instance or a Properties file.
More details can be found in the [Connection Properties](#connection-properties) section of this document.

* If neither a Properties object nor a file are supplied, connections will be attempted insecurely to the default url, `nats://localhost:4222`.  
* The properties are used to create a JNats client Options object. 

2\. When configuring JetStream, if your stream has a domain or prefix, you can supply options used to create a
[JetStreamOptions](https://github.com/nats-io/nats.java/blob/main/src/main/java/io/nats/client/JetStreamOptions.java) object.
More details can be found in the [JetStream Options](#jetstream-options) section of this document.

3\. You can configure with code or from JSON or YAML files, or a combination.

Since a configuration property can be set from a property _and_ can be set directly with a fluent builder method,
the last one called will be used. For example, there is a property `sink_converter_class_name` where you can specify the sink converter class.
If _first_ you call `jsonConfigFile(...)` with a file that has that key, 
and _then_ you call `sinkConverterClass(...)` the value set with `sinkConverterClass` method will be used.

4\. Some builders have both "set" and "add" methods. 
* If you `set(...)` you replace whatever was in place before with the new configuration.
* If you `add(...)` you append the new configuration to whatever was already set or added. 

## Connection Properties

There are two ways to configure connection properties.

1\. Passing a file location via `.connectionPropertiesFile(String)`
This is probably the most likely way that you will configure the connection.

**Configuration files must be available on all Flink nodes that might run execute the job,**
otherwise, an IOException will be thrown when trying to open the file, and the node will fail to run.

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

For full connection properties see the [NATS - Java Client, README Options Properties](https://github.com/nats-io/nats.java?tab=readme-ov-file#properties)

### Example Connection Properties

This is a simple example [Connection Properties File](src/examples/resources/connection.properties) you can review.

This is an example of using properties to set up tls:
```properties
io.nats.client.url=tls://myhost:4222
io.nats.client.keyStorePassword=kspassword
io.nats.client.keyStore=/path/to/keystore.jks
io.nats.client.trustStorePassword=tspassword
io.nats.client.trustStore=/path/to/truststore.jks
```

**When a property references a file location, those files must be present on all nodes.**
 
### JetStream Options
The following properties can be set in the connection Properties object or file and are required 
if your streams live in domains. See the server documentation for more info.  

| Property              | JetStreamOptions Builder Method           |
|-----------------------|-------------------------------------------|
| `jso_prefix`          | `prefix(String prefix)`                   | 
| `jso_domain`          | `domain(String domain)`                   |

## Source and Sink Concepts

The output of a source becomes the input of a sink. This project's sources get messages from NATS 
and emits them in a form that can be received by a sink. They are generic, 
meaning you can provide a converter to emit the type expected by your target source.

The sink receives input from a source. This project's sink uses converters to process data it receives from the source 
and extract message data and headers, which will then be published to NATS by the sink.
This project's sinks are generic, meaning you can provide a converter to handle emitted type.

There will probably be a time when you read data from a source other than NATS or write data to a source
other than NATS. It will be up to you to understand what a foreign source outputs or a foreign sink expects
as input. 

Maybe a non-NATS source emits an encrypted array of bytes. If you are just storing that data as is as the payload of 
a NATS message, you can use the built-in `ByteArraySourceConverter`. Otherwise, using that class as your starting
point, you can customize what it does with that encrypted input. 

Maybe you want to sink a subject or stream of data to some non-NATS sink. Maybe that foreign sink is just a passthrough
for message data in either byte or String form, then you can use the provided sink converters, for instance a
`ByteArraySinkConverter` But maybe you have headers in your messages, and you combine those headers, 
the subject and the into some JSON. Then you will need to provide a custom sink converter.

A converter will receive the entire NATS `Message` so you can extract all the information you need.
If you are using the JetStreamSource, the message will contain metadata, including the stream sequence.

## Converters

All converter implementations must implement either SourceConverter or SinkConverter interfaces.

A [SourceConverter](src/main/java/io/synadia/flink/message/SourceConverter.java) takes a message as input and outputs 
the type that sinks are expecting.
```java
public interface SourceConverter<OutputT> extends Serializable, ResultTypeQueryable<OutputT> {
  /**
   * Read a message and to create an instance of the output type.
   * @return the output object
   */
  OutputT convert(Message message);
}
```

A [SinkConverter](src/main/java/io/synadia/flink/message/SinkConverter.java) takes the input from the source
and returns a SinkMessage that can have a byte[] for the message payload and headers in a Headers object.
```java
public interface SinkConverter<InputT> extends Serializable {
  /**
   * Create a SinkMessage based on the input object given to the sink.
   * If you return null, no messages will be published for this input.
   * @param input the input object
   * @return The SinkMessage object or null.
   */
  SinkMessage convert(InputT input);
}
```

These converters are supplied with the project;

| Class                                                                                                | Use                                |
|------------------------------------------------------------------------------------------------------|------------------------------------|
| [AsciiStringSourceConverter](src/main/java/io/synadia/flink/message/AsciiStringSourceConverter.java) | Message payload is an ASCII string |
| [Utf8StringSourceConverter](src/main/java/io/synadia/flink/message/Utf8StringSourceConverter.java)   | Message payload is a UTF-8 string  |
| [ByteArraySourceConverter](src/main/java/io/synadia/flink/message/ByteArraySourceConverter.java)     | Message payload is binary.         |
| [AsciiStringSinkConverter](src/main/java/io/synadia/flink/message/AsciiStringSinkConverter.java)     | Sink input is an ASCII string.     |
| [Utf8StringSinkConverter](src/main/java/io/synadia/flink/message/Utf8StringSinkConverter.java)       | Sink input is a UTF-8 string.      |
| [ByteArraySinkConverter](src/main/java/io/synadia/flink/message/ByteArraySinkConverter.java)         | Sink input is a byte array.        |


It will be likely that you want to supply a custom converter. A good place to start is by looking at the provided converters.

Something important to remember is that your converters must be Serializable. If you don't have any state or configuration
to save, this is a non-issue. If you do have some state to save, look at the String converters; their state is the name
of the character set.

## Sinks

Both the `NatsSink` and `JetStreamSink` expect input from a source
and use a converter to extract data and optionally headers from that input and produce a `SinkMessage`.
The `SinkMessage` is then published.

The difference between `NatsSink` and `JetStreamSink` is that `NatsSink` publishes a message to a subject that it
assumes is not part of a JetStream stream. It "fires and forgets". On the other hand, the `JetStreamSink` publishes
the message to what it assumes to be a JetStream subject and waits for a Publish Ack. If the publishing fails,
the sink throws an exception.

Subjects may not have wildcards. The builder will not validate this, it's up to you to ensure you configure
this properly, otherwise publishing will fail during runtime with an exception.

JetStream subjects must exist in your target NATS system otherwise publishing will fail during runtime with an exception. 

To construct a `NatsSink`, you must use the `NatsSinkBuilder`.
To construct a `JetStreamSink`, you must use the `JetStreamSinkBuilder`.
The sinks can be configured in code or from files on JSON or YAML format. They support these property keys:
* `sink_converter_class_name`
* `subjects`

## Sources

There are two types of Flink source implementations available. 
1. NatsSource, which subscribes to NATS core subjects.
2. JetStreamSource, which consumes JetStream stream subjects.

Subjects with wildcards are allowed and are treated as one subject for splits. This is normal NATS behavior.
If a subject is invalid or the supplied stream does not have a matching subject, 
consuming will fail during runtime with an exception.

### NatsSource

A `NatsSource` subscribes to one or more core NATS subjects and uses a 
Source Converter implementation to convert the messages to the output type emitted to sinks.

* Each subject is subscribed in its own split, which Flink can run in different threads or in different nodes depending
on your Flink configuration.

* A NatsSource is currently only unbounded, meaning it runs forever. 
It's on the TODO list to make this the source able to be bounded,
meaning limited to run for a number of messages or period of time. 

To construct a `NatsSource`, you must use the `NatsSourceBuilder`. 
The source can be configured in code or from files on JSON or YAML format. It supports these property keys:
* `source_converter_class_name`
* `subjects`

### JetStreamSource

A `JetStreamSource` consumes to one or more JetStream subjects and uses a
Source Converter implementation to convert the messages to the output type emitted to sinks.

* Each subject is consumed in its own split, which Flink can run in different threads or in different nodes depending
  on your Flink configuration.

To construct a `JetStreamSource`, you must use the `JetStreamSourceBuilder`.

A `JetStreamSource` is composed of a `SourceConverter` and one or more `JetStreamSubjectConfiguration` instances.

All instances of JetStreamSubjectConfiguration must be of the same boundedness, meaning they must all be configured
with a maximum number of messages to read or none of them are configured with a maximum.

#### JetStreamSubjectConfiguration

A `JetStreamSubjectConfiguration` is created by using the JetStreamSubjectConfiguration Builder

```java
JetStreamSubjectConfiguration subjectConfiguration = 
    JetStreamSubjectConfiguration.builder()
        ...
        .build()
```

* A configuration requires a stream name and a subject. 
* You can optionally supply a start sequence or start time, which will have the effect of starting
the consumption at that point in the stream. A start time can be in any format that `java.time.ZonedDateTime` can parse.
See [ZonedDateTime.parse](https://docs.oracle.com/javase/8/docs/api/java/time/ZonedDateTime.html#parse-java.lang.CharSequence-)
* You can specify a maximum number of messages to read. This has the effect of making the source BOUNDED.
* You can specify "ack mode" which will ack groups of messages at checkpoints. This is significantly slower.
You must specify ack mode to true if your stream is a work queue
* You can specify the consumer batch size and threshold percent if you feel you need to tune the behavior of the consumer.   

The source can be configured in code or from files on JSON or YAML format. It supports these property keys:
```json
{
  "source_converter_class_name": "io.synadia.flink.message.Utf8StringSourceConverter",
  "jetstream_subject_configurations": [
    {
      "stream_name": "streamName",
      "subject": "subject1",
      "start_sequence":  999,
      "start_time":  "2025-04-08T00:38:32.109526400Z",
      "max_messages_to_read": 10000,
      "ack_mode":  false,
      "batch_size": 100,
      "threshold_percent": 25
    },
    {
      "stream_name": "streamName",
      "subject": "subject2"
    },
    {
      "stream_name": "anotherStream",
      "subject": "foo.>"
    },
    {
      "stream_name": "anotherStream",
      "subject": "bar.*"
    }
  ]
}
```
```yaml
---
source_converter_class_name: io.synadia.flink.message.Utf8StringSourceConverter
jetstream_subject_configurations:
- stream_name: streamName
  subject: subject1
  start_sequence: 999
  start_time: '2025-04-08T00:38:32.109526400Z'
  max_messages_to_read: 10000
  ack_mode: false
  batch_size: 100
  threshold_percent: 25
- stream_name: streamName
  subject: subject2
- stream_name: anotherStream
  subject: foo.>
- stream_name: anotherStream
  subject: bar.*
```

Here are 
the [JSON](src/examples/resources/core-source-config.json) 
and [YAML](src/examples/resources/core-source-config.yaml)
configuration files used in the examples

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
