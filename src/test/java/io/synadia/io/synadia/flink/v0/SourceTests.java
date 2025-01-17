// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.io.synadia.flink.v0;

import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.synadia.flink.v0.payload.ByteArrayPayloadDeserializer;
import io.synadia.flink.v0.payload.StringPayloadDeserializer;
import io.synadia.flink.v0.payload.StringPayloadSerializer;
import io.synadia.flink.v0.sink.NatsSink;
import io.synadia.flink.v0.sink.NatsSinkBuilder;
import io.synadia.flink.v0.source.NatsSource;
import io.synadia.flink.v0.source.NatsSourceBuilder;
import io.synadia.io.synadia.flink.Publisher;
import io.synadia.io.synadia.flink.TestBase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SourceTests extends TestBase {
    @Test
    public void testSourceWithString() throws Exception {
        final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());
        String sourceSubject1 = random();
        String sourceSubject2 = random();
        String sinkSubject = random();

        runInServer(true, (nc, url) -> {
            // listen to the sink output
            Dispatcher d = nc.createDispatcher();
            d.subscribe(sinkSubject, syncList::add);

            // publish to the source's subjects
            Publisher publisher = new Publisher(nc, sourceSubject1, sourceSubject2);
            new Thread(publisher).start();

            // --------------------------------------------------------------------------------
            Properties connectionProperties = defaultConnectionProperties(url);
            StringPayloadDeserializer deserializer = new StringPayloadDeserializer();
            NatsSourceBuilder<String> builder = new NatsSourceBuilder<String>()
                .subjects(sourceSubject1, sourceSubject2)
                .payloadDeserializer(deserializer)
                .connectionProperties(connectionProperties);

            NatsSource<String> natsSource = builder.build();
            StreamExecutionEnvironment env = getStreamExecutionEnvironment();
            DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-string-input");

            NatsSink<String> sink = newNatsStringSink(sinkSubject, connectionProperties, null);
            ds.sinkTo(sink);

            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
            env.executeAsync("testSourceWithString");

            Thread.sleep(1000);

            env.close();

            publisher.stop();

            validate(syncList, sourceSubject1, sourceSubject2);
        });
    }

    @Test
    public void testSourceWithByteArray() throws Exception {
        final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());
        String sourceSubject1 = random();
        String sourceSubject2 = random();
        String sinkSubject = random();

        runInServer(true, (nc, url) -> {
            // listen to the sink output
            Dispatcher d = nc.createDispatcher();
            d.subscribe(sinkSubject, syncList::add);

            // publish to the source's subjects
            Publisher publisher = new Publisher(nc, sourceSubject1, sourceSubject2);
            new Thread(publisher).start();

            // --------------------------------------------------------------------------------
            Properties connectionProperties = defaultConnectionProperties(url);
            ByteArrayPayloadDeserializer deserializer = new ByteArrayPayloadDeserializer();
            NatsSourceBuilder<Byte[]> builder = new NatsSourceBuilder<Byte[]>()
                .subjects(sourceSubject1, sourceSubject2)
                .payloadDeserializer(deserializer)
                .connectionProperties(connectionProperties);

            NatsSource<Byte[]> natsSource = builder.build();
            StreamExecutionEnvironment env = getStreamExecutionEnvironment();
            DataStream<Byte[]> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-byte-array-input");

            NatsSink<Byte[]> sink = newNatsByteArraySink(sinkSubject, connectionProperties, null);
            ds.sinkTo(sink);

            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
            env.executeAsync("testSourceWithByteArray");

            Thread.sleep(1000);

            env.close();

            publisher.stop();

            validate(syncList, sourceSubject1, sourceSubject2);
        });
    }

    private static void validate(List<Message> syncList, String sourceSubject1, String sourceSubject2) {
        boolean hasSourceSubject1 = false;
        boolean hasSourceSubject2 = false;
        for (Message m : syncList) {
            String payload = new String(m.getData());
            if (payload.contains(sourceSubject1)) {
                hasSourceSubject1 = true;
            }
            else if (payload.contains(sourceSubject2)) {
                hasSourceSubject2 = true;
            }
        }

        assertTrue(hasSourceSubject1);
        assertTrue(hasSourceSubject2);
    }

    static class HeaderAwareStringPayloadDeserializer extends StringPayloadDeserializer {
        @Override
        public String getObject(String subject, byte[] input, Headers headers) {
            String hSubject = headers.getFirst("subject");
            String hNum = headers.getFirst("num");
            return Publisher.dataString(hSubject, hNum);
        }
    }

    @Test
    public void testSourceWithHeaders() throws Exception {
        final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());
        String sourceSubject1 = random();
        String sourceSubject2 = random();
        String sinkSubject = random();

        runInServer(true, (nc, url) -> {
            // listen to the sink output
            Dispatcher d = nc.createDispatcher();
            d.subscribe(sinkSubject, syncList::add);

            // publish to the source's subjects
            Publisher publisher = new Publisher(nc,
                (subject, num) -> {
                    Headers h = new Headers();
                    h.put("subject", subject);
                    h.put("num", num.toString());
                    return h;
                },
                sourceSubject1, sourceSubject2);
            new Thread(publisher).start();

            // --------------------------------------------------------------------------------
            Properties connectionProperties = defaultConnectionProperties(url);
            HeaderAwareStringPayloadDeserializer deserializer = new HeaderAwareStringPayloadDeserializer();
            NatsSourceBuilder<String> builder = new NatsSourceBuilder<String>()
                .subjects(sourceSubject1, sourceSubject2)
                .payloadDeserializer(deserializer)
                .connectionProperties(connectionProperties);

            NatsSource<String> natsSource = builder.build();
            StreamExecutionEnvironment env = getStreamExecutionEnvironment();
            DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-headers-input");

            final StringPayloadSerializer serializer = new StringPayloadSerializer();
            NatsSinkBuilder<String> sinkBuilder = new NatsSinkBuilder<String>()
                .subjects(sinkSubject)
                .payloadSerializer(serializer);
            sinkBuilder.connectionProperties(connectionProperties);

            NatsSink<String> sink = sinkBuilder.build();

            ds.sinkTo(sink);

            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
            env.executeAsync("testSourceWithHeaders");

            Thread.sleep(1000);

            env.close();

            publisher.stop();

            validate(syncList, sourceSubject1, sourceSubject2);
        });
    }
}
