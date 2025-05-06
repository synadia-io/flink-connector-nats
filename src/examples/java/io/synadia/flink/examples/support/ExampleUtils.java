// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.examples.support;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.utils.PropertiesUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ExampleUtils {
    public static final String EXAMPLES_CONNECTION_PROPERTIES_FILE = "src/examples/resources/connection.properties";
    public static final String SOURCE_PROPS_FILE = "src/examples/resources/core-source.properties";
    public static final String SINK_PROPS_FILE = "src/examples/resources/core-sink.properties";

    public static Connection connect(String propsFile) throws IOException, InterruptedException {
        return connect(PropertiesUtils.loadPropertiesFromFile(propsFile));
    }

    public static Connection connect(Properties props) throws IOException, InterruptedException {
        Options options = new Options.Builder()
            .properties(props)
            .connectionListener(new ExampleConnectionListener())
            .errorListener(new ExampleErrorListener())
            .build();
        return Nats.connect(options);
    }

    public static void createOrReplaceStream(Connection nc, StorageType storageType, String stream, String... subjects) throws IOException, JetStreamApiException {
        createOrReplaceStream(nc.jetStreamManagement(), storageType, stream, Arrays.asList(subjects));
    }

    public static void createOrReplaceStream(JetStreamManagement jsm, StorageType storageType, String stream, String... subjects) throws IOException, JetStreamApiException {
        createOrReplaceStream(jsm, storageType, stream, Arrays.asList(subjects));
    }

    public static void createOrReplaceStream(Connection nc, StorageType storageType, String stream, List<String> subjects) throws IOException, JetStreamApiException {
        createOrReplaceStream(nc.jetStreamManagement(), storageType, stream, subjects);
    }

    public static void createOrReplaceStream(JetStreamManagement jsm, StorageType storageType, String stream, List<String> subjects) throws IOException, JetStreamApiException {
        try {
            jsm.deleteStream(stream);
        }
        catch (Exception ignore) {}

        StreamConfiguration streamConfig = StreamConfiguration.builder()
            .name(stream)
            .subjects(subjects)
            .storageType(storageType)
            .build();
        jsm.addStream(streamConfig);
        System.out.println("Stream created: " + stream);
    }

    public static String format(long l) {
        return String.format("%,d", l);
    }

    public static String humanTime(long millis) {
        // HHh mmm sss ms
        long h = millis / 3600000;
        long left = millis - (h * 3600000);
        long m = left / 60000;
        left = left - (m * 60000);
        long s = left / 1000;
        left = left - (s * 1000);

        if (h > 0) {
            return h + ":" + pad2(m) + ":" + pad2(s) + "." + pad3(left);
        }

        if (m > 0) {
            return m + ":" + pad2(s) + "." + pad3(left);
        }
        if (s > 0) {
            return s + "." + pad3(left) + " sec";
        }
        return millis + " ms";
    }

    public static String pad3(long n) {
        return n > 100
            ? "" + n
            : n < 10 ? "00" + n : "0" + n;
    }

    public static String pad2(long n) {
        return n < 10 ? "0" + n : "" + n;
    }

    public static void reportSinkListener(Map<String, AtomicInteger> receivedMap, int manualTotal) {
        StringBuilder sb = new StringBuilder("Received | ");
        int total = 0;
        List<String> sorted = new ArrayList<>(receivedMap.keySet());
        sorted.sort(String.CASE_INSENSITIVE_ORDER);
        for (String sortedSubject : sorted) {
            int count = receivedMap.get(sortedSubject).get();
            if (total > 0) {
                sb.append(", ");
            }
            total += count;
            sb.append(sortedSubject)
                .append("/")
                .append(count);
        }
        sb.append(" | Total: ")
            .append(ExampleUtils.format(total))
            .append(" (")
            .append(manualTotal)
            .append(")");
        System.out.println(sb);
    }

    public static void writeToFile(String filename, String text) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            writer.write(text);
            writer.flush();
        }
    }
}