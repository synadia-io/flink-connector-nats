package io.synadia.flink.examples.v0;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.v0.utils.PropertiesUtils;

import java.io.IOException;
import java.util.Properties;

public class ManagedExample1Setup {

    public static final String NAME_PREFIX = "example1-";

    public static final Properties PROPS;
    public static final String SOURCE_SUBJECT_1;
    public static final String SOURCE_SUBJECT_2;
    public static final String SOURCE_SUBJECT_3;
    public static final String SINK_SUBJECT;
    public static final String STREAM_NAME;
    public static final String CONSUMER_NAME;

    static {
        try {
            // Load configuration from application.properties
            PROPS = PropertiesUtils.loadPropertiesFromFile("src/examples/resources/application.properties");

            // Define static names loaded from properties
            SOURCE_SUBJECT_1 = NAME_PREFIX + "source.1";
            SOURCE_SUBJECT_2 = NAME_PREFIX + "source.2";
            SOURCE_SUBJECT_3 = NAME_PREFIX + "source.3";
            SINK_SUBJECT = NAME_PREFIX + "sink";
            STREAM_NAME = NAME_PREFIX + "stream";
            CONSUMER_NAME = NAME_PREFIX + "consumer";
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        // Connect to NATS server
        try (Connection nc = connect(PROPS)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // Create a JetStream stream for the source subject
            createStream(jsm, STREAM_NAME, SOURCE_SUBJECT_1, SOURCE_SUBJECT_2, SOURCE_SUBJECT_3);

            // Publish test messages to the source subject
            publish(js, SOURCE_SUBJECT_1, 100000);
            publish(js, SOURCE_SUBJECT_2, 100000);
            publish(js, SOURCE_SUBJECT_3, 100000);

            // Create a consumer for the JetStream source
            createConsumer(jsm, STREAM_NAME, CONSUMER_NAME, SOURCE_SUBJECT_1, SOURCE_SUBJECT_2, SOURCE_SUBJECT_3);
        }
    }

    /**
     * Connect to the NATS server using provided properties.
     */
    public static Connection connect(Properties props) throws Exception {
        return Nats.connect(props.getProperty("io.nats.client.url"));
    }

    /**
     * Create a JetStream stream with the specified name and subject.
     */
    public static void createStream(JetStreamManagement jsm, String streamName, String... subjects) throws Exception {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(streamName)
                .subjects(subjects)
                .build();
        jsm.addStream(streamConfig);
        System.out.println("Stream created: " + streamName);
    }

    /**
     * Create a durable consumer for the given stream and subject.
     */
    public static void createConsumer(JetStreamManagement jsm, String streamName, String consumerName, String... subjects) throws Exception {
        ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                .durable(consumerName) // Durable consumer for persistence
                .ackPolicy(AckPolicy.All) // Explicit acknowledgement policy
                .filterSubjects(subjects) // Filter messages for this subject
                .build();
        jsm.addOrUpdateConsumer(streamName, consumerConfig);
        System.out.println("Consumer created: " + consumerName);
    }

    /**
     * Publish a fixed number of test messages to the specified JetStream subject.
     */
    public static void publish(JetStream js, String subject, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            js.publish(subject, ("Message " + i).getBytes());
        }
    }
}