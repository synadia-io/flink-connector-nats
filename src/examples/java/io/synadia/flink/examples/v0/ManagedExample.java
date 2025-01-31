package io.synadia.flink.examples.v0;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.synadia.flink.examples.support.Publisher;
import io.synadia.flink.v0.payload.StringPayloadDeserializer;
import io.synadia.flink.v0.sink.NatsSink;
import io.synadia.flink.v0.sink.NatsSinkBuilder;
import io.synadia.flink.v0.source.ManagedSource;
import io.synadia.flink.v0.source.ManagedSourceBuilder;
import io.synadia.flink.v0.source.ManagedSubjectConfiguration;
import io.synadia.flink.v0.utils.PropertiesUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static io.synadia.flink.examples.v0.ExampleUtils.connect;

public class ManagedExample extends ManagedExample1Setup {
    public static final String SINK_SUBJECT = EXAMPLE_NAME + "-sink";
    public static final int PARALLELISM = 5; // if 0 or less, parallelism will not be set
    public static final int RUN_TIME = 10000; // millis

    public static void main(String[] args) throws Exception {
        // load properties from a file for example application.properties
        Properties props = PropertiesUtils.loadPropertiesFromFile("src/examples/resources/application.properties");

        // make a connection to publish and listen with
        // props has io.nats.client.url in it
        Connection nc = connect(props);
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();

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

        ManagedSubjectConfiguration msc11 = ManagedSubjectConfiguration.builder()
            .streamName(STREAM1_NAME)
            .subject(STREAM1_SUBJECT1)
            .build();

        ManagedSubjectConfiguration msc12 = ManagedSubjectConfiguration.builder()
            .streamName(STREAM1_NAME)
            .subject(STREAM1_SUBJECT2)
            .build();

        ManagedSubjectConfiguration msc21 = ManagedSubjectConfiguration.builder()
            .streamName(STREAM2_NAME)
            .subject(STREAM2_SUBJECT1)
            .build();

        ManagedSubjectConfiguration msc22 = ManagedSubjectConfiguration.builder()
            .streamName(STREAM2_NAME)
            .subject(STREAM2_SUBJECT2)
            .build();

        ManagedSource<String> source = new ManagedSourceBuilder<String>()
            .subjectConfigurations(msc11, msc12, msc21, msc22)
            .payloadDeserializer(new StringPayloadDeserializer())
            .connectionProperties(props)
            .build();
        System.out.println(source);

        // create sink
        NatsSink<String> sink = new NatsSinkBuilder<String>()
            .sinkProperties(props)
            .connectionProperties(props)
            .subjects(sinkSubject) // subject comes last because the builder uses the last input
            .build();
        System.out.println(sink);

        // setup and start flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), EXAMPLE_NAME);
        dataStream.sinkTo(sink);
        env.executeAsync(EXAMPLE_NAME);

        Thread.sleep(RUN_TIME);

        for (Map.Entry<String, AtomicInteger> entry : receivedMap.entrySet()) {
            System.out.println("Messages received for subject '" + entry.getKey() + "' : " + entry.getValue().get());
        }
        System.exit(0);
    }
}
