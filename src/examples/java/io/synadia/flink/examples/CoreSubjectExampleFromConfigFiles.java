package io.synadia.flink.examples;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.synadia.flink.examples.support.ExampleUtils;
import io.synadia.flink.examples.support.Publisher;
import io.synadia.flink.sink.NatsSink;
import io.synadia.flink.sink.NatsSinkBuilder;
import io.synadia.flink.source.NatsSource;
import io.synadia.flink.source.NatsSourceBuilder;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * This is the same workflow as CoreSubjectExample, but it creates the source and sink
 * object using a JSON or YAML file instead of being coded.
 * Run the CoreSubjectExample first, and it will generate the config files, overwriting
 * the ones packaged in the examples' resource directory. See the constants in
 * CoreSubjectExample for file locations.
 */
public class CoreSubjectExampleFromConfigFiles {
    public static final String JOB_NAME = "csefcf";

    // ------------------------------------------------------------------------------------------
    // Set this flag to tell the program to use the JSON or YAML file for input.
    // true means use the JSON. false means use the YAML
    // The input file is determined by the constants in JetStreamExampleHelper.java
    // either SOURCE_CONFIG_FILE_JSON or SOURCE_CONFIG_FILE_YAML
    // ------------------------------------------------------------------------------------------
    public static final boolean USE_JSON_NOT_YAML = true;

    public static void main(String[] args) throws Exception {

        // ==========================================================================================
        // Create a NatsSource source
        // ==========================================================================================
        // A NatSources subscribes to a core subject for messages.
        // ------------------------------------------------------------------------------------------
        // Build the source by setting up the connection properties, the deserializer
        // and subjects.
        // ------------------------------------------------------------------------------------------
        NatsSource<String> source;
        NatsSourceBuilder<String> sourceBuilder = new NatsSourceBuilder<String>()
            .connectionPropertiesFile(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE);
        if (USE_JSON_NOT_YAML) {
            source = sourceBuilder.sourceJson(CoreSubjectExample.SOURCE_CONFIG_FILE_JSON).build();
            System.out.println("Source as configured via JSON\n" + source.toJson());
        }
        else {
            source = sourceBuilder.sourceYaml(CoreSubjectExample.SOURCE_CONFIG_FILE_YAML).build();
            System.out.println("Source as configured via Yaml\n" + source.toYaml());
        }

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
        NatsSink<String> sink;
        NatsSinkBuilder<String> sinkBuilder = new NatsSinkBuilder<String>()
            .connectionPropertiesFile(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE);
        if (USE_JSON_NOT_YAML) {
            sink = sinkBuilder.sinkJson(CoreSubjectExample.SINK_CONFIG_FILE_JSON).build();
            System.out.println("Sink as configured via JSON\n" + sink.toJson());
        }
        else {
            sink = sinkBuilder.sinkYaml(CoreSubjectExample.SINK_CONFIG_FILE_YAML).build();
            System.out.println("Sink as configured via Yaml\n" + sink.toYaml());
        }

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
