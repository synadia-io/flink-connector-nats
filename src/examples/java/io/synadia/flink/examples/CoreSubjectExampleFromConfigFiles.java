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
    public static final String EXAMPLE_NAME = "Example";

    // ------------------------------------------------------------------------------------------
    // Set this flag to tell the program to use the JSON or YAML file for input.
    // true means use the JSON. false means use the YAML
    // The input file is determined by the constants in JetStreamExampleHelper.java
    // either SOURCE_CONFIG_FILE_JSON or SOURCE_CONFIG_FILE_YAML
    // ------------------------------------------------------------------------------------------
    public static final boolean USE_JSON_NOT_YAML = false;

    public static void main(String[] args) throws Exception {

        // Create the source.
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

        // Create the sink.
        NatsSink<String> sink;
        NatsSinkBuilder<String> sinkBuilder = new NatsSinkBuilder<String>()
            .connectionPropertiesFile(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE);
        if (USE_JSON_NOT_YAML) {
            sink = sinkBuilder.sinkJson(JetStreamExample.SINK_CONFIG_FILE_JSON).build();
            System.out.println("Sink as configured via JSON\n" + sink.toJson());
        }
        else {
            sink = sinkBuilder.sinkYaml(JetStreamExample.SINK_CONFIG_FILE_YAML).build();
            System.out.println("Sink as configured via Yaml\n" + sink.toYaml());
        }

        // make a connection to publish and listen with
        // props has io.nats.client.url in it
        Connection nc = ExampleUtils.connect(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE);

        // start publishing to where the source will get
        // the source will have missed some messages by the time it gets running
        // but that's typical for a non-stream subject and something for
        // the developer to plan for
        Publisher publisher = new Publisher(nc, source.getSubjects());
        new Thread(publisher).start();

        // listen for messages that the sink publishes
        List<String> sinkSubjects = sink.getSubjects();
        System.out.println("Setting up core subscriptions to the following subjects: " + sinkSubjects);
        Dispatcher dispatcher = nc.createDispatcher(m -> {
            System.out.println("Sink listener received message on subject: '" + m.getSubject() + "' with data: '"  + new String(m.getData()) + "'");
        });
        for (String subject : sinkSubjects) {
            dispatcher.subscribe(subject);
        }

        // setup and start flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "NatsSource");
        dataStream.sinkTo(sink);

        env.executeAsync(EXAMPLE_NAME);

        // run for 10 seconds
        Thread.sleep(10_000);

        publisher.stop();
        env.close();
        System.exit(0);
    }
}
