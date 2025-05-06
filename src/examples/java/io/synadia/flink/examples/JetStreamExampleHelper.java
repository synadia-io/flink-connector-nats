// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.examples;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.synadia.flink.examples.support.ExampleUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.synadia.flink.examples.support.Publisher.makePayload;

public class JetStreamExampleHelper {

    public static final String SOURCE_A_STREAM = "source-a";
    public static final String SOURCE_B_STREAM = "source-b";
    public static final String SOURCE_A_SUBJECT = "ua";
    public static final String[] SOURCE_B_SUBJECTS = new String[]{"ub1","ub2","ub3","ub4"};
    public static final int SOURCE_A_MESSAGE_COUNT = 100_000;
    public static final int[] SOURCE_B_MESSAGE_COUNTS = new int[]{30_000,70_000,50_000,90_000};
    public static final StorageType SOURCE_A_STORAGE_TYPE = StorageType.Memory;
    public static final StorageType SOURCE_B_STORAGE_TYPE = StorageType.Memory;

    public static final int SOURCES_TOTAL_SUBJECT_COUNT;
    public static final int SOURCES_TOTAL_MESSAGES;
    public static final int SOURCES_MOST_MESSAGES_ANY_SUBJECT;

    public static final String SINK_STREAM_NAME = "sink";
    public static final String SINK_SUBJECT = "uk";
    public static final StorageType SINK_STORAGE_TYPE = StorageType.Memory;

    static {
        SOURCES_TOTAL_SUBJECT_COUNT = SOURCE_A_MESSAGE_COUNT + SOURCE_B_MESSAGE_COUNTS.length;
        int total = SOURCE_A_MESSAGE_COUNT;
        int most = SOURCE_A_MESSAGE_COUNT;
        for (int count : SOURCE_B_MESSAGE_COUNTS) {
            total += count;
            most = Math.max(most, count);
        }
        SOURCES_TOTAL_MESSAGES = total;
        SOURCES_MOST_MESSAGES_ANY_SUBJECT = most;
    }

    public static void setupSinkStream(Connection nc) throws IOException, JetStreamApiException {
        System.out.println("Setting up sink stream.");
        ExampleUtils.createOrReplaceStream(nc, SINK_STORAGE_TYPE, SINK_STREAM_NAME, SINK_SUBJECT);
    }

    public static void setupDataStreams(Connection nc) throws Exception {
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();

        // Create a JetStream stream for the source subject
        System.out.println("Setting up data streams");
        ExampleUtils.createOrReplaceStream(jsm, SOURCE_A_STORAGE_TYPE, SOURCE_A_STREAM, SOURCE_A_SUBJECT);
        ExampleUtils.createOrReplaceStream(jsm, SOURCE_B_STORAGE_TYPE, SOURCE_B_STREAM, SOURCE_B_SUBJECTS);

        // Publish test messages to the source subject
        System.out.println("Publishing...");
        int countA = 0;
        int[] countB = new int[SOURCE_B_MESSAGE_COUNTS.length];
        List<CompletableFuture<PublishAck>> futures = new ArrayList<>();
        for (int n = 1; n <= SOURCES_MOST_MESSAGES_ANY_SUBJECT; n++) {
            if (countA < SOURCE_A_MESSAGE_COUNT) {
                countA++;
                futures.add(js.publishAsync(SOURCE_A_SUBJECT, makePayload(SOURCE_A_SUBJECT, n).getBytes()));
            }
            for (int x = 0; x < SOURCE_B_MESSAGE_COUNTS.length; x++) {
                if (countB[x] < SOURCE_B_MESSAGE_COUNTS[x]) {
                    String subject = SOURCE_B_SUBJECTS[x];
                    futures.add(js.publishAsync(subject, makePayload(subject, n).getBytes()));
                    countB[x]++;
                }
            }
            if (n % 1000 == 0) {
                for (CompletableFuture<PublishAck> future : futures) {
                    future.get(1000, TimeUnit.MILLISECONDS);
                }
                futures.clear();
            }
            if (n % 10000 == 0) {
                printSetupCounts(countA, countB);
            }
        }
        for (CompletableFuture<PublishAck> future : futures) {
            future.get(1000, TimeUnit.MILLISECONDS);
        }
        printSetupCounts(countA, countB);
        System.out.println("Publishing complete.");
    }

    private static void printSetupCounts(int countA, int[] countB) {
        System.out.print(SOURCE_A_SUBJECT + "/" + countA);
        for (int x = 0; x < SOURCE_B_MESSAGE_COUNTS.length; x++) {
            System.out.print(", ");
            System.out.print(SOURCE_B_SUBJECTS[x] + "/" + countB[x]);
        }
        System.out.println();
    }
}