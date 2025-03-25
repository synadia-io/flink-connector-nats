package io.synadia.flink.examples.support;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

import java.io.IOException;
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

    public static void createOrReplaceStream(JetStreamManagement jsm, StorageType storageType, String stream, String... subjects) throws IOException, JetStreamApiException {
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
}