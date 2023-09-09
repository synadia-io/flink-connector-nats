// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import io.nats.client.*;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BasicTests extends TestBase {

    @Test
    public void testInServer() throws Exception {
        runInServer((nc, url) -> {
            String subject = random();
            Subscriber sub = new Subscriber(nc, subject);

            Properties props = new Properties();
            props.put(Options.PROP_URL, url);

            StreamExecutionEnvironment env = getStreamExecutionEnvironment();

            final StringPayloadSerializer serializer = new StringPayloadSerializer();
            final NatsSubjectSink<String> natsSubjectSink = new NatsSubjectSink<>(serializer, props, subject);
            final SinkWriter<String> writer = natsSubjectSink.createWriter(null);

            DataStream<String> text = getStringDataStream(env);

            text.sinkTo(natsSubjectSink);

            writer.close();
            env.execute("InServerTest");

            sub.assertAllMessagesReceived();
        });
    }

    static class Subscriber implements MessageHandler {
        public final Dispatcher d;
        final Map<String, Integer> resultMap = new HashMap<>();

        public Subscriber(Connection nc, String subject) {
            d = nc.createDispatcher();
            d.subscribe(subject, this);
        }

        @Override
        public void onMessage(Message message) throws InterruptedException {
            String payload = new String(message.getData());
            resultMap.merge(payload.toLowerCase(), 1, Integer::sum);
        }

        public void assertAllMessagesReceived() {
            assertEquals(WORD_COUNT_MAP.size(), resultMap.size());
            for (String key : resultMap.keySet()) {
                Integer ri = resultMap.get(key);
                Integer tri = WORD_COUNT_MAP.get(key);
                assertEquals(ri, tri);
            }
        }
    }
}
