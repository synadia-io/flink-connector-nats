package io.synadia.flink.examples.v0;

import io.nats.client.*;
import io.synadia.flink.v0.payload.StringPayloadDeserializer;
import io.synadia.flink.v0.payload.StringPayloadSerializer;
import io.synadia.flink.v0.sink.NatsSink;
import io.synadia.flink.v0.sink.NatsSinkBuilder;
import io.synadia.flink.v0.source.NatsJetStreamSource;
import io.synadia.flink.v0.source.NatsJetStreamSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ManagedExample1 extends ManagedExample1Setup {
    public static void main(String[] args) throws Exception {
        // Load configuration from application.properties
        Properties connectionProperties = ManagedExample1Setup.PROPS;

        // Connect to NATS server
        Connection nc = connect(connectionProperties);
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();

        // List to capture sink messages received via the NATS dispatcher
        final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());

        // Configure the NATS JetStream Source
        StringPayloadDeserializer deserializer = new StringPayloadDeserializer();
        NatsJetStreamSourceBuilder<String> builder = new NatsJetStreamSourceBuilder<String>()
                .subjects(SOURCE_SUBJECT_1, SOURCE_SUBJECT_2, SOURCE_SUBJECT_3)
                .payloadDeserializer(deserializer) // Deserialize messages from source
                .connectionProperties(connectionProperties)
                .streamName(STREAM_NAME)
                .consumerName(CONSUMER_NAME)
                .maxFetchRecords(100)
                .maxFetchTime(Duration.ofSeconds(5))
                .boundness(Boundedness.BOUNDED);

        NatsJetStreamSource<String> natsSource = builder.build();

        // Configure Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getCheckpointConfig().setCheckpointInterval(10_000L); // Set checkpoint interval
        DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-input");

        // Create a NATS dispatcher to listen to sink messages
        Dispatcher dispatcher = nc.createDispatcher();
        dispatcher.subscribe(SINK_SUBJECT, syncList::add); // Collect sink messages

        // Configure the NATS JetStream Sink
        NatsSink<String> sink = new NatsSinkBuilder<String>()
                .subjects(SINK_SUBJECT)
                .connectionProperties(connectionProperties)
                .payloadSerializer(new StringPayloadSerializer()) // Serialize messages for sink
                .build();
        ds.sinkTo(sink);

        // Configure Flink restart strategy
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));

        // Execute Flink pipeline asynchronously
        env.executeAsync("JetStream Source-to-Sink Example");

        // Allow the job to run for 12 seconds
        Thread.sleep(12_000);

        // Gracefully close the dispatcher and Flink environment
        dispatcher.unsubscribe(SINK_SUBJECT);
        env.close();

        // Print received sink messages
        for (Message m : syncList) {
            String payload = new String(m.getData());
            System.out.println("Received message at sink: " + payload);
        }

        // Delete the stream after the test
        jsm.deleteStream(STREAM_NAME);
        System.out.println("Stream deleted: " + STREAM_NAME);

        // Close the NATS connection
        nc.close();

        // Terminate the application
        System.exit(0);
    }
}