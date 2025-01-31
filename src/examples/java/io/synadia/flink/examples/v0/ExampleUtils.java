package io.synadia.flink.examples.v0;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.examples.support.ExampleConnectionListener;
import io.synadia.flink.examples.support.ExampleErrorListener;

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
}