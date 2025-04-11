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

public class CoreSubjectExample {
    public static final String EXAMPLE_NAME = "Example";

    public static void main(String[] args) throws Exception {

        // create source
        NatsSource<String> source = new NatsSourceBuilder<String>()
            .sourceProperties(ExampleUtils.SOURCE_PROPS_FILE)
            .connectionPropertiesFile(ExampleUtils.CONNECTION_PROPS_FILE)
            .build();
        System.out.println(source);

        // create sink
        NatsSink<String> sink = new NatsSinkBuilder<String>()
            .sinkProperties(ExampleUtils.SINK_PROPS_FILE)
            .connectionPropertiesFile(ExampleUtils.CONNECTION_PROPS_FILE)
            .build();
        System.out.println(sink);

        // make a connection to publish and listen with
        // props has io.nats.client.url in it
        Connection nc = ExampleUtils.connect(ExampleUtils.CONNECTION_PROPS_FILE);

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
