// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.examples.jetstream;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.synadia.flink.examples.support.ExampleUtils;

import static io.synadia.flink.examples.support.Publisher.makePayload;

public class JetStreamExampleSetup extends JetStreamExampleBase {

    public static void main(String[] args) throws Exception {
        // Make a connection. PROPS has key "io.nats.client.url" in it.
        // See ExampleUtils.connect(...) for props usage.
        try (Connection nc = ExampleUtils.connect(PROPS)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // Create a JetStream stream for the source subject
            ExampleUtils.createOrReplaceStream(jsm, SOURCE_A_STORAGE_TYPE, SOURCE_A_STREAM, SOURCE_A_SUBJECT);
            ExampleUtils.createOrReplaceStream(jsm, SOURCE_B_STORAGE_TYPE, SOURCE_B_STREAM, SOURCE_B_SUBJECTS);

            // Publish test messages to the source subject
            System.out.println("Publishing...");
            int countA = 0;
            int[] countB = new int[SOURCE_B_MESSAGE_COUNTS.length];
            for (int n = 1; n <= SOURCES_MOST_MESSAGES_ANY_SUBJECT; n++) {
                if (countA < SOURCE_A_MESSAGE_COUNT) {
                    countA++;
                    js.publish(SOURCE_A_SUBJECT, makePayload(SOURCE_A_SUBJECT, n).getBytes());
                }
                for (int x = 0; x < SOURCE_B_MESSAGE_COUNTS.length; x++) {
                    if (countB[x] < SOURCE_B_MESSAGE_COUNTS[x]) {
                        String subject = SOURCE_B_SUBJECTS[x];
                        js.publish(subject, makePayload(subject, n).getBytes());
                        countB[x]++;
                    }
                }
                if (n % 10000 == 0) {
                    printCounts(countA, countB);
                }
            }
            printCounts(countA, countB);
        }
    }

    private static void printCounts(int countA, int[] countB) {
        System.out.print(SOURCE_A_SUBJECT + "/" + countA);
        for (int x = 0; x < SOURCE_B_MESSAGE_COUNTS.length; x++) {
            System.out.print(", ");
            System.out.print(SOURCE_B_SUBJECTS[x] + "/" + countB[x]);
        }
        System.out.println();
    }
}