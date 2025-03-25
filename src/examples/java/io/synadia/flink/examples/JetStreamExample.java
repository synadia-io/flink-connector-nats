package io.synadia.flink.examples;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.synadia.flink.examples.support.Publisher;
import io.synadia.flink.payload.StringPayloadDeserializer;
import io.synadia.flink.sink.NatsSink;
import io.synadia.flink.sink.NatsSinkBuilder;
import io.synadia.flink.source.JetStreamSource;
import io.synadia.flink.source.JetStreamSourceBuilder;
import io.synadia.flink.source.JetStreamSubjectConfiguration;
import io.synadia.flink.utils.PropertiesUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.synadia.flink.examples.support.ExampleUtils.connect;
import static io.synadia.flink.examples.support.ExampleUtils.humanTime;

public class JetStreamExample extends JetStreamExampleSetup {
    public static final String EXAMPLE_NAME = "mngd-un";
    public static final String SINK_SUBJECT = "mngd-un-sink";
    public static final int PARALLELISM = 1; // if 0 or less, parallelism will manually set
    public static final int REPORT_FREQUENCY = 10000;

    public static final int CHECKPOINTING_INTERVAL = 100;

    // < 1 makes the source Boundedness.CONTINUOUS_UNBOUNDED
    // > 0 makes the source Boundedness.BOUNDED by giving it a maximum number of messages to read
    public static final int MAX_MESSAGES_TO_READ = 10_000;

    @SuppressWarnings("resource")
    public static void main(String[] args) throws Exception {
        // load properties from a file for example application.properties
        Properties props = PropertiesUtils.loadPropertiesFromFile("src/examples/resources/application.properties");

        // Make a connection. props has key "io.nats.client.url" in it. See connect(...) for props usage.
        Connection nc = connect(props);
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();

        // listen for messages that the sink publishes
        AtomicLong lastMessageReceived = new AtomicLong(System.currentTimeMillis() + 5000); // 5000 gives it a little time to get started
        AtomicInteger allCounter = new AtomicInteger(0);
        Map<String, AtomicInteger> receivedMap = new HashMap<>();
        Dispatcher dispatcher = nc.createDispatcher(m -> {
            String data = new String(m.getData());
            String publishedSubject = Publisher.extractSubject(data);
            AtomicInteger count = receivedMap.computeIfAbsent(publishedSubject, k -> new AtomicInteger());
            count.incrementAndGet();
            lastMessageReceived.set(System.currentTimeMillis());
            if (allCounter.incrementAndGet() % REPORT_FREQUENCY == 0) {
//                reportSinkListener(receivedMap, allCounter);
            }
        });
        dispatcher.subscribe(SINK_SUBJECT);

        List<JetStreamSubjectConfiguration> subjectConfigurationsA = JetStreamSubjectConfiguration.builder()
            .streamName(STREAM_A_NAME)
            .maxMessagesToRead(MAX_MESSAGES_TO_READ)
            .buildWithSubjects(STREAM_A_SUBJECTS);

        List<JetStreamSubjectConfiguration> subjectConfigurationsB = JetStreamSubjectConfiguration.builder()
            .streamName(STREAM_B_NAME)
            .maxMessagesToRead(MAX_MESSAGES_TO_READ)
            .buildWithSubjects(STREAM_B_SUBJECTS);

        JetStreamSource<String> source = new JetStreamSourceBuilder<String>()
            .addSubjectConfigurations(subjectConfigurationsA)
            .addSubjectConfigurations(subjectConfigurationsB)
            .payloadDeserializer(new StringPayloadDeserializer())
            .connectionProperties(props)
            .build();
        System.out.println(source);

        // create sink
        NatsSink<String> sink = new NatsSinkBuilder<String>()
            .sinkProperties(props)
            .connectionProperties(props)
            .subjects(SINK_SUBJECT) // subject comes last because the builder uses the last input
            .build();
        System.out.println(sink);

        // setup and start flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINTING_INTERVAL);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(CHECKPOINTING_INTERVAL * 7 / 10);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        if (PARALLELISM > 0) {
            env.setParallelism(PARALLELISM);
        }

        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), EXAMPLE_NAME);
        dataStream.sinkTo(sink);
        long start = System.currentTimeMillis();
        env.executeAsync(EXAMPLE_NAME);

        // Just wait until we stop getting messages
        long sinceLastMessage = 0;
        do {
            Thread.sleep(100);
            sinceLastMessage = System.currentTimeMillis() - lastMessageReceived.get();
        } while (allCounter.get() == 0 || sinceLastMessage < 1000);

        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Elapsed Time: " + humanTime(elapsed));

        System.out.println();
        reportSinkListener(receivedMap, allCounter);
        System.exit(0);
    }

    private static void reportSinkListener(Map<String, AtomicInteger> receivedMap, AtomicInteger allCounter) {
        for (Map.Entry<String, AtomicInteger> entry : receivedMap.entrySet()) {
            System.out.println("Messages received by subject '" + entry.getKey() + "' : " + entry.getValue().get());
        }
        System.out.println("Sink listener received " + allCounter.get() + " total messages.");
    }
}
