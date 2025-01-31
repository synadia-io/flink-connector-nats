package io.synadia.flink.examples.v0;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StorageType;
import io.synadia.flink.v0.utils.PropertiesUtils;

import java.util.Properties;

import static io.synadia.flink.examples.support.Publisher.makePayload;
import static io.synadia.flink.examples.v0.ExampleUtils.connect;
import static io.synadia.flink.examples.v0.ExampleUtils.createOrReplaceStream;

public class ManagedExample1Setup {

    public static final String EXAMPLE_NAME = "managed1";
    public static final String STREAM1_NAME = EXAMPLE_NAME + "-stream-1";
    public static final String STREAM2_NAME = EXAMPLE_NAME + "-stream-2";
    public static final String STREAM1_SUBJECT1 = EXAMPLE_NAME + "-subject-1-1";
    public static final String STREAM1_SUBJECT2 = EXAMPLE_NAME + "-subject-1-2";
    public static final String STREAM2_SUBJECT1 = EXAMPLE_NAME + "-subject-2-1";
    public static final String STREAM2_SUBJECT2 = EXAMPLE_NAME + "-subject-2-2";
    public static final StorageType STORAGE_TYPE = StorageType.File;

    public static void main(String[] args) throws Exception {
        // Connect to NATS server
        Properties props = PropertiesUtils.loadPropertiesFromFile("src/examples/resources/application.properties");

        try (Connection nc = connect(props)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // Create a JetStream stream for the source subject
            createOrReplaceStream(jsm, STORAGE_TYPE, STREAM1_NAME, STREAM1_SUBJECT1, STREAM1_SUBJECT2);
            createOrReplaceStream(jsm, STORAGE_TYPE, STREAM2_NAME, STREAM2_SUBJECT1, STREAM2_SUBJECT2);

            // Publish test messages to the source subject
            System.out.println("Publishing...");
            for (int i = 1; i <= 100_000; i++) {
                js.publish(STREAM1_SUBJECT1, makePayload("data11", i).getBytes());
                js.publish(STREAM1_SUBJECT1, makePayload("data12", i).getBytes());
                js.publish(STREAM1_SUBJECT1, makePayload("data21", i).getBytes());
                js.publish(STREAM1_SUBJECT1, makePayload("data22", i).getBytes());
                if (i % 1000 == 0) {
                    System.out.println("Published " + i + " per subject.");
                }
            }
        }
    }
}