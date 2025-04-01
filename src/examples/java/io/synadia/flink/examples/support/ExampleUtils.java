// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.examples.support;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ExampleUtils {

    public static Connection connect(Properties props) throws Exception {
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

    public static String[] toStringArray(String commaDelimited) {
        return commaDelimited.split(",");
    }

    public static int[] toIntArray(String commaDelimited) {
        String[] strings = toStringArray(commaDelimited);
        int[] ints = new int[strings.length];
        for (int i = 0; i < strings.length; i++) {
            ints[i] = Integer.parseInt(strings[i]);
        }
        return ints;
    }
}