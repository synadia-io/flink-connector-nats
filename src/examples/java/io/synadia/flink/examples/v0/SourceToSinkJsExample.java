package io.synadia.flink.examples.v0;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.v0.payload.StringPayloadDeserializer;
import io.synadia.flink.v0.payload.StringPayloadSerializer;
import io.synadia.flink.v0.sink.NatsSink;
import io.synadia.flink.v0.sink.NatsSinkBuilder;
import io.synadia.flink.v0.source.NatsJetStreamSource;
import io.synadia.flink.v0.source.NatsJetStreamSourceBuilder;
import io.synadia.flink.v0.utils.PropertiesUtils;
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

public class SourceToSinkJsExample {
    public static void main(String[] args) throws Exception {
        // Load configuration from application.properties
        Properties props = PropertiesUtils.loadPropertiesFromFile("src/examples/resources/application.properties");

        // Define static names loaded from properties
        String sourceSubject = props.getProperty("source.JsSubject");
        String sinkSubject = props.getProperty("sink.JsSubject");
        String streamName = props.getProperty("source.stream");
        String consumerName = props.getProperty("source.consumer");

        // Connect to NATS server
        Connection nc = connect(props);
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();

        // Create a JetStream stream for the source subject
        createStream(jsm, streamName, sourceSubject);

        // Publish test messages to the source subject
        publish(js, sourceSubject, 10);

        // Create a consumer for the JetStream source
        createConsumer(jsm, streamName, sourceSubject, consumerName);

        // List to capture sink messages received via the NATS dispatcher
        final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());

        // Configure the NATS JetStream Source
        Properties connectionProperties = props;
        StringPayloadDeserializer deserializer = new StringPayloadDeserializer();
        NatsJetStreamSourceBuilder<String> builder = new NatsJetStreamSourceBuilder<String>()
                .subjects(sourceSubject)
                .payloadDeserializer(deserializer) // Deserialize messages from source
                .connectionProperties(connectionProperties)
                .consumerName(consumerName)
                .maxFetchRecords(100)
                .maxFetchTime(Duration.ofSeconds(5))
                .boundness(Boundedness.BOUNDED);

        NatsJetStreamSource<String> natsSource = builder.build();

        // Configure Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(10_000L); // Set checkpoint interval
        DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-input");

        // Create a NATS dispatcher to listen to sink messages
        Dispatcher dispatcher = nc.createDispatcher();
        dispatcher.subscribe(sinkSubject, syncList::add); // Collect sink messages

        // Configure the NATS JetStream Sink
        NatsSink<String> sink = new NatsSinkBuilder<String>()
                .subjects(sinkSubject)
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
        dispatcher.unsubscribe(sinkSubject);
        env.close();

        // Print received sink messages
        for (Message m : syncList) {
            String payload = new String(m.getData());
            System.out.println("Received message at sink: " + payload);
        }

        // Delete the stream after the test
        jsm.deleteStream(streamName);
        System.out.println("Stream deleted: " + streamName);

        // Close the NATS connection
        nc.close();

        // Terminate the application
        System.exit(0);
    }

    /**
     * Connect to the NATS server using provided properties.
     */
    private static Connection connect(Properties props) throws Exception {
        return Nats.connect(props.getProperty("io.nats.client.url"));
    }

    /**
     * Create a JetStream stream with the specified name and subject.
     */
    private static void createStream(JetStreamManagement jsm, String streamName, String subject) throws Exception {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(streamName)
                .subjects(subject)
                .build();
        jsm.addStream(streamConfig);
        System.out.println("Stream created: " + streamName);
    }

    /**
     * Create a durable consumer for the given stream and subject.
     */
    private static void createConsumer(JetStreamManagement jsm, String streamName, String subject, String consumerName) throws Exception {
        ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                .durable(consumerName) // Durable consumer for persistence
                .ackPolicy(AckPolicy.All) // Explicit acknowledgement policy
                .filterSubject(subject) // Filter messages for this subject
                .build();
        jsm.addOrUpdateConsumer(streamName, consumerConfig);
        System.out.println("Consumer created: " + consumerName);
    }

    /**
     * Publish a fixed number of test messages to the specified JetStream subject.
     */
    private static void publish(JetStream js, String subject, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            js.publish(subject, ("Message " + i).getBytes());
        }
    }
}