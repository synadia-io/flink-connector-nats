package io.synadia.flink.examples;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.synadia.flink.examples.support.Publisher;
import io.synadia.flink.sink.NatsSink;
import io.synadia.flink.sink.NatsSinkBuilder;
import io.synadia.flink.source.NatsSource;
import io.synadia.flink.source.NatsSourceBuilder;
import io.synadia.flink.utils.PropertiesUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.synadia.flink.examples.support.ExampleUtils.connect;

public class SourceToSinkExample2 {
    private static final Logger LOG = LoggerFactory.getLogger(SourceToSinkExample2.class);

    public static final String EXAMPLE_NAME = "Example2";

    public static final int NUM_SOURCE_SUBJECTS = 8;
    public static final int PARALLELISM = 5; // if 0 or less, parallelism will not be set
    public static final int RUN_TIME = 3000; // millis
    public static final long PUBLISH_DELAY = 250; // miils
    public static final int PUBLISH_MESSAGE_COUNT_JITTER = 3; // will publish 1 to n messages per subject each publish loop

    public static void main(String[] args) throws Exception {
        // load properties from a file for example application.properties
        Properties props = PropertiesUtils.loadPropertiesFromFile("src/examples/resources/application.properties");

        // make a connection to publish and listen with
        // props has io.nats.client.url in it
        Connection nc = connect(props);

        // start publishing to where the source will get
        // the source will have missed some messages by the time it gets running
        // but that's typical for a non-stream subject and something for
        // the developer to plan for
        List<String> sourceSubjects = new ArrayList<>();
        for (int s = 1; s <= NUM_SOURCE_SUBJECTS; s++) {
            sourceSubjects.add("source." + s);
        }
        Publisher publisher = new Publisher(nc, sourceSubjects, false, PUBLISH_DELAY, PUBLISH_MESSAGE_COUNT_JITTER);
        new Thread(publisher).start();

        // listen for messages that the sink publishes
        Map<String, AtomicInteger> receivedMap = new HashMap<>();
        Dispatcher dispatcher = nc.createDispatcher(m -> {
            String data = new String(m.getData());
            String publishedSubject = Publisher.extractSubject(data);
            AtomicInteger count = receivedMap.computeIfAbsent(publishedSubject, k -> new AtomicInteger());
            count.incrementAndGet();
            // LOG.info("Listening to `{}` got message `{}`", m.getSubject(), data); // comment back in so see every message the sink publishes
        });
        String sinkSubject = "sink-target";
        dispatcher.subscribe(sinkSubject);

        // create source
        NatsSource<String> source = new NatsSourceBuilder<String>()
            .sourceProperties(props)
            .connectionProperties(props)
            .subjects(sourceSubjects) // subjects come last because the builder uses the last input
            .build();
        LOG.info("{}", source);

        // create sink
        NatsSink<String> sink = new NatsSinkBuilder<String>()
            .sinkProperties(props)
            .connectionProperties(props)
            .subjects(sinkSubject) // subject comes last because the builder uses the last input
            .build();
        LOG.info("{}", sink);

        // setup and start flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        if (PARALLELISM > 0) {
            env.setParallelism(PARALLELISM);
        }

        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), EXAMPLE_NAME);
        dataStream.sinkTo(sink);
        env.executeAsync(EXAMPLE_NAME);

        Thread.sleep(RUN_TIME);

        publisher.stop();
        env.close();

        for (Map.Entry<String, AtomicInteger> entry : receivedMap.entrySet()) {
            LOG.info("Messages received for subject '{}' : {}", entry.getKey(), entry.getValue().get());
        }
        System.exit(0);
    }
}
