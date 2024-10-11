// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.io.synadia.flink;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;
import io.nats.client.support.SerializableConsumerConfiguration;
import io.synadia.flink.Debug;
import io.synadia.flink.Utils;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.sink.NatsSink;
import io.synadia.flink.source.NatsJetStreamSource;
import io.synadia.flink.source.NatsJetstreamSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.nats.client.api.ConsumerConfiguration.INTEGER_UNSET;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NatsJetStreamSourceTest extends TestBase {

    static void publish(JetStream js, String subject, int count) throws Exception {
        publish(js, subject, count, 0);
    }

    static void publish(JetStream js, String subject, int count, long delay) throws Exception {
        for (int x = 0; x < count; x++) {
            js.publish(subject, ("data-" + subject + "-" + x + "-" + random()).getBytes());
            if (delay > 0) {
                sleep(delay);
            }
        }
    }

    @Test
    public void testJsSourceBounded() throws Exception {
        final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());
        String sourceSubject = random("sub");
        String sinkSubject = random("sink");
        String streamName = random("strm");
        String consumerName = random("con");

        runInServer(true, (nc, url) -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = jsm.jetStream();

            createStream(jsm, streamName, sourceSubject);
            publish(js, sourceSubject, 10);

            ConsumerConfiguration cc = createConsumer(jsm, streamName, sourceSubject, consumerName, INTEGER_UNSET);

            // --------------------------------------------------------------------------------
            PayloadDeserializer<String> deserializer = new WriteData();
            SerializableConsumerConfiguration scc = new SerializableConsumerConfiguration();
            scc.setConsumerConfiguration(cc);
            NatsJetstreamSourceBuilder<String> builder = new NatsJetstreamSourceBuilder<String>()
                .deserializationSchema(deserializer)
                .consumerConfig(scc)
                .natsUrl(url)
                .subject(sourceSubject);

            NatsJetStreamSource<String> natsSource = builder.build();
            StreamExecutionEnvironment env = getStreamExecutionEnvironment();
            env.getCheckpointConfig().setCheckpointInterval(10_000L);
            DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-input");

            // listen to the sink output
            Dispatcher d = nc.createDispatcher();
            d.subscribe(sinkSubject, syncList::add);

            Properties connectionProperties = defaultConnectionProperties(url);
            NatsSink<String> sink = newNatsSink(sinkSubject, connectionProperties, null);
            ds.sinkTo(sink);

//            ds.map(String::toUpperCase); //To Avoid Sink Dependency
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
            env.executeAsync("TestJsSourceBounded");

            Thread.sleep(12_000);
            env.close();
            ConsumerInfo ci = jsm.getConsumerInfo(streamName, consumerName);
            SequenceInfo sequenceInfo = ci.getDelivered();
            assertTrue(sequenceInfo.getStreamSequence() >= 2);

            Debug.info("SL", syncList.size());
            for (Message m : syncList) {
                String payload = new String(m.getData());
                Debug.info("PAY", payload);
            }
        });
    }

    @Test
    public void testJsSourceUnbounded() throws Exception {
        String sourceSubject = random("sub");
        String streamName = random("strm");
        String consumerName = random("con");

        runInServer(true, (nc, url) -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = jsm.jetStream();
            createStream(jsm, streamName, sourceSubject);

            ConsumerConfiguration cc = createConsumer(jsm, streamName, sourceSubject, consumerName, 5);
            // --------------------------------------------------------------------------------
            PayloadDeserializer<String> deserializer = new WriteData();
            SerializableConsumerConfiguration scc = new SerializableConsumerConfiguration();
            scc.setConsumerConfiguration(cc);
            NatsJetstreamSourceBuilder<String> builder = new NatsJetstreamSourceBuilder<String>()
                .deserializationSchema(deserializer)
                .consumerConfig(scc)
                .natsUrl(url)
                .subject(sourceSubject);

            // Flink environment setup
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            Properties connectionProperties = defaultConnectionProperties(url);
            SerializableConsumerConfiguration consumerConfig = new SerializableConsumerConfiguration(cc);

            DataStream<String> ds = env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "nats-source-input");
            ds.map(String::toUpperCase);

            // Running Flink job in a separate thread
            Thread flinkThread = new Thread(() -> {
                try {
                    env.execute("TestJsSourceUnbounded");
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            });
            flinkThread.start();

            publish(js, sourceSubject, 5, 100);

            Thread.sleep(10000); // Increased sleep time to ensure messages are processed
            SequenceInfo sequenceInfo = nc.jetStream().getConsumerContext(sourceSubject, consumerName).getConsumerInfo().getDelivered();
            assertTrue(sequenceInfo.getStreamSequence() >= 5);
            flinkThread.interrupt(); // Interrupt to stop the Flink job
        });
    }

    private static ConsumerConfiguration createConsumer(JetStreamManagement jsm, String streamName, String sourceSubject, String consumerName, int maxBatch) throws IOException, JetStreamApiException {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(consumerName)
                .ackPolicy(AckPolicy.All)
                .filterSubject(sourceSubject)
                .maxBatch(5)
                .build();
        jsm.addOrUpdateConsumer(streamName, cc);
        return cc;
    }

    private static void createStream(JetStreamManagement jsm, String streamName, String sourceSubject) throws IOException, JetStreamApiException {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
            .name(streamName)
            .subjects(sourceSubject)
            .storageType(StorageType.Memory)
            .build();
        jsm.addStream(streamConfig);
    }
}

class WriteData implements PayloadDeserializer<String> {
    @Override
    public String getObject(String subject, byte[] input, Headers headers) {
        return new String(input);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Utils.getTypeInformation(String.class);
    }
}
