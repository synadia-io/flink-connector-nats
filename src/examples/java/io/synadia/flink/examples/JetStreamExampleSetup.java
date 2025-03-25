package io.synadia.flink.examples;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StorageType;
import io.synadia.flink.utils.PropertiesUtils;

import java.util.Properties;

import static io.synadia.flink.examples.support.ExampleUtils.connect;
import static io.synadia.flink.examples.support.ExampleUtils.createOrReplaceStream;
import static io.synadia.flink.examples.support.Publisher.makePayload;

public class JetStreamExampleSetup {

    public static final String STREAM_A_NAME = "stream-a";
    public static final String STREAM_B_NAME = "stream-b";
    public static final String[] STREAM_A_SUBJECTS = new String[]{"ua1", "ua2", "ua3", "ua4", "ua5"};
    public static final String[] STREAM_B_SUBJECTS = new String[]{"ub1", "ub2", "ub3", "ub4", "ub5"};
    public static final int[] MESSAGES_A = new int[]{30_000, 50_000, 70_000, 90_000};
    public static final int[] MESSAGES_B = new int[]{20_000, 40_000, 60_000, 80_000};
    public static final int SUBJECT_COUNT;
    public static final int TOTAL_MESSAGES;
    public static final int MOST_MESSAGES_ANY_SUBJECT;
    public static final StorageType STORAGE_TYPE = StorageType.Memory;

    static {
        SUBJECT_COUNT = MESSAGES_A.length + MESSAGES_B.length;
        int total = 0;
        int most = 0;
        for (int count : MESSAGES_A) {
            total += count;
            most = Math.max(most, count);
        }
        for (int count : MESSAGES_B) {
            total += count;
            most = Math.max(most, count);
        }
        TOTAL_MESSAGES = total;
        MOST_MESSAGES_ANY_SUBJECT = most;
    }

    @SuppressWarnings({"ConstantValue", "DataFlowIssue"})
    public static void main(String[] args) throws Exception {
        // load properties from a file for example application.properties
        Properties props = PropertiesUtils.loadPropertiesFromFile("src/examples/resources/application.properties");

        // Make a connection. props has key "io.nats.client.url" in it. See connect(...) for props usage.
        try (Connection nc = connect(props)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // Create a JetStream stream for the source subject
            createOrReplaceStream(jsm, STORAGE_TYPE, STREAM_A_NAME, STREAM_A_SUBJECTS);
            createOrReplaceStream(jsm, STORAGE_TYPE, STREAM_B_NAME, STREAM_B_SUBJECTS);

            // Publish test messages to the source subject
            System.out.println("Publishing...");
            int[] countA = new int[MESSAGES_A.length];
            int[] countB = new int[MESSAGES_B.length];
            for (int i = 1; i <= MOST_MESSAGES_ANY_SUBJECT; i++) {
                for (int x = 0; x < MESSAGES_A.length; x++) {
                    if (countA[x] < MESSAGES_A[x]) {
                        String subject = STREAM_A_SUBJECTS[x];
                        js.publish(subject, makePayload(subject, i).getBytes());
                        countA[x]++;
                    }
                }
                for (int x = 0; x < MESSAGES_B.length; x++) {
                    if (countB[x] < MESSAGES_B[x]) {
                        String subject = STREAM_B_SUBJECTS[x];
                        js.publish(subject, makePayload(subject, i).getBytes());
                        countB[x]++;
                    }
                }
                if (i % 10000 == 0) {
                    printCounts(countA, countB);
                }
            }
            printCounts(countA, countB);
        }
    }

    private static void printCounts(int[] countA, int[] countB) {
        for (int x = 0; x < MESSAGES_A.length; x++) {
            if (x > 0) {
                System.out.print(", ");
            }
            System.out.print(STREAM_A_SUBJECTS[x] + " " + countA[x]);
        }
        for (int x = 0; x < MESSAGES_B.length; x++) {
            System.out.print(", ");
            System.out.print(STREAM_B_SUBJECTS[x] + " " + countB[x]);
        }
        System.out.println();
    }
}