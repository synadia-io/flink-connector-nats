package io.synadia.flink.examples;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.synadia.flink.examples.support.ExampleUtils;
import io.synadia.flink.examples.support.Publisher;
import io.synadia.flink.sink.NatsSink;
import io.synadia.flink.sink.NatsSinkBuilder;
import io.synadia.flink.source.NatsSource;
import io.synadia.flink.source.NatsSourceBuilder;
import io.synadia.flink.utils.Constants;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

import static io.synadia.flink.examples.support.ExampleUtils.writeToFile;

public class CoreSubjectExample {
    // ==========================================================================================
    // General Configuration: Use these settings to change how the example runs
    // ==========================================================================================
    public static final String JOB_NAME = "cse";

    // ------------------------------------------------------------------------------------------
    // Locations where to write config files based on how the example gets configured.
    // These files can be used in the JetStreamExampleFromConfigFiles example.
    // ------------------------------------------------------------------------------------------
    public static final String SOURCE_CONFIG_FILE_JSON = "src/examples/resources/core-source-config.json";
    public static final String SOURCE_CONFIG_FILE_YAML = "src/examples/resources/core-source-config.yaml";
    public static final String SINK_CONFIG_FILE_JSON = "src/examples/resources/core-sink-config.json";
    public static final String SINK_CONFIG_FILE_YAML = "src/examples/resources/core-sink-config.yaml";

    public static void main(String[] args) throws Exception {

        // ==========================================================================================
        // Create a NatsSource source
        // ==========================================================================================
        // A NatSources subscribes to a core subject for messages.
        // ------------------------------------------------------------------------------------------
        // Build the source by setting up the connection properties, the deserializer
        // and subjects.
        // ------------------------------------------------------------------------------------------
        // A StringPayloadDeserializer takes the NATS Message and outputs its data payload as a String
        // When we published to these streams, the data is in the form "data--<subject>--<num>"
        // ------------------------------------------------------------------------------------------
        NatsSource<String> source = new NatsSourceBuilder<String>()
            .connectionPropertiesFile(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE)
            .payloadDeserializerClass(Constants.STRING_PAYLOAD_DESERIALIZER_CLASSNAME)
            .subjects("source1", "source2")
            .build();

        // ------------------------------------------------------------------------------------------
        // Here we write the source config out to a file in different formats.
        // The files can be used in the JetStreamExampleFromConfigFiles example.
        // ------------------------------------------------------------------------------------------
        writeToFile(SOURCE_CONFIG_FILE_JSON, source.toJson());
        writeToFile(SOURCE_CONFIG_FILE_YAML, source.toYaml());

        // ==========================================================================================
        // Create a NatsSink sink
        // ==========================================================================================
        // A NatsSink sink publishes to a subject
        // ------------------------------------------------------------------------------------------
        // When we published to the source subject, the data was in the form "data--<subject>--<num>"
        // The sink takes that payload and publishes it as the message payload
        // to all the sink subjects. For this example there are two sink subjects.
        // ------------------------------------------------------------------------------------------
        // We have one sink for all those source subjects. This means that all messages from
        // all those sources get "sinked" to the same JetStream subject
        // This may or may not be a real use-case, it's here for example.
        // ------------------------------------------------------------------------------------------
        NatsSink<String> sink = new NatsSinkBuilder<String>()
            .connectionPropertiesFile(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE)
            .payloadSerializerClass(Constants.STRING_PAYLOAD_SERIALIZER_CLASSNAME)
            .subjects("sink1", "sink2")
            .build();
        writeToFile(SINK_CONFIG_FILE_JSON, sink.toJson());
        writeToFile(SINK_CONFIG_FILE_YAML, sink.toYaml());

        // ==========================================================================================
        // Publishing to source subjects and subscribing to sink subjects.
        // ==========================================================================================
        // The source gets the message payload, and then flink passes it to the sink. Then the sink
        // formulates its own message and then publishes to all of its subjects.
        // * We publish, so the source has messages to listen to.
        // * We subscribe to the same subjects that the sink publishes to, to show the round trip
        // ------------------------------------------------------------------------------------------
        // We then just start publishing to the same subjects the source is configured
        // to subscribe to. The source will have missed some messages by the time it gets running,
        // but that's typical for a non-stream subject and something for the developer to plan for.
        // ------------------------------------------------------------------------------------------

        // ------------------------------------------------------------------------------------------
        // Make a connection to use similar to how the sink or source do it by using connection
        // properties. See ExampleUtils.connect
        // ------------------------------------------------------------------------------------------
        Connection nc = ExampleUtils.connect(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE);

        // ------------------------------------------------------------------------------------------
        // Start publishing.
        // ------------------------------------------------------------------------------------------
        Publisher publisher = new Publisher(nc, source.getSubjects());
        new Thread(publisher).start();

        // ------------------------------------------------------------------------------------------
        // Listen for messages that the sink publishes. The sink may publish to multiple subjects.
        // We subscribe to all those subjects. This is just normal nats subscribing, but it's how
        // we can validate the round trip.
        // ------------------------------------------------------------------------------------------
        List<String> sinkSubjects = sink.getSubjects();
        System.out.println("Setting up core subscriptions to the following subjects: " + sinkSubjects);
        Dispatcher dispatcher = nc.createDispatcher(m -> {
            System.out.println("Sink listener received message on subject: '" + m.getSubject() + "' with data: '"  + new String(m.getData()) + "'");
        });
        for (String subject : sinkSubjects) {
            dispatcher.subscribe(subject);
        }

        // ==========================================================================================
        // Setup and start flink
        // ==========================================================================================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "NatsSource");
        dataStream.sinkTo(sink);

        env.executeAsync(JOB_NAME);

        // ------------------------------------------------------------------------------------------
        // Run for 10 seconds
        // ------------------------------------------------------------------------------------------
        Thread.sleep(10_000);
        publisher.stop();
        env.close();
        System.exit(0);
    }
}
