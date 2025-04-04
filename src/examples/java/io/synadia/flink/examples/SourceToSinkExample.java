package io.synadia.flink.examples;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.synadia.flink.examples.support.ExampleUtils;
import io.synadia.flink.examples.support.Publisher;
import io.synadia.flink.sink.NatsSink;
import io.synadia.flink.sink.NatsSinkBuilder;
import io.synadia.flink.source.NatsSource;
import io.synadia.flink.source.NatsSourceBuilder;
import io.synadia.flink.utils.PropertiesUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Properties;

public class SourceToSinkExample {
    public static void main(String[] args) throws Exception {
        // load properties from a file for example application.properties
        Properties props = PropertiesUtils.loadPropertiesFromFile("src/examples/resources/application.properties");

        // make a connection to publish and listen with
        // props has io.nats.client.url in it
        Connection nc = ExampleUtils.connect(props);

        // start publishing to where the source will get
        // the source will have missed some messages by the time it gets running
        // but that's typical for a non-stream subject and something for
        // the developer to plan for
        List<String> sourceSubjects = PropertiesUtils.getPropertyAsList(props, "source.subjects");
        Publisher publisher = new Publisher(nc, sourceSubjects);
        new Thread(publisher).start();

        // listen for messages that the sink publishes
        Dispatcher dispatcher = nc.createDispatcher(m -> {
            System.out.printf("Listening. Subject: %s MessageRecord: %s\n", m.getSubject(), new String(m.getData()));
        });
        List<String> sinkSubjects = PropertiesUtils.getPropertyAsList(props, "sink.subjects");
        for (String subject : sinkSubjects) {
            dispatcher.subscribe(subject);
        }

        // create source
        NatsSource<String> source = new NatsSourceBuilder<String>()
            .sourceProperties(props)
            .connectionProperties(props)
            .build();
        System.out.println(source);

        // create sink
        NatsSink<String> sink = new NatsSinkBuilder<String>()
            .sinkProperties(props)
            .connectionProperties(props)
            .build();
        System.out.println(sink);

        // setup and start flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "NatsSource");
        dataStream.sinkTo(sink);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
        env.executeAsync("Example");

        // run for 10 seconds
        Thread.sleep(10_000);

        publisher.stop();
        env.close();
        System.exit(0);
    }
}
