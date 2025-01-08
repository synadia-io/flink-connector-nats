// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.io.synadia.flink.v0;

import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.synadia.flink.v0.payload.StringPayloadDeserializer;
import io.synadia.flink.v0.sink.NatsSink;
import io.synadia.flink.v0.source.NatsSource;
import io.synadia.flink.v0.source.NatsSourceBuilder;
import io.synadia.flink.v0.utils.ConnectionProperties;
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
    public void testSource() throws Exception {
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
                .connectionProperties(new ConnectionProperties<>(connectionProperties));

            NatsSource<String> natsSource = builder.build();
            StreamExecutionEnvironment env = getStreamExecutionEnvironment();
            DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-input");

            NatsSink<String> sink = newNatsSink(sinkSubject, new ConnectionProperties<>(connectionProperties));
            ds.sinkTo(sink);

            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
            env.executeAsync("TestSource");

            Thread.sleep(1000);

            env.close();

            publisher.stop();

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
        });
    }
}
