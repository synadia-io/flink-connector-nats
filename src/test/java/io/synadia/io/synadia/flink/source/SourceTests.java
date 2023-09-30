// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.io.synadia.flink.source;

import io.nats.client.Connection;
import io.nats.client.Options;
import io.synadia.flink.payload.StringPayloadDeserializer;
import io.synadia.flink.source.NatsSource;
import io.synadia.flink.source.NatsSourceBuilder;
import io.synadia.io.synadia.flink.TestBase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class SourceTests extends TestBase {

    @Test
    public void testSource() throws Exception {
        runInServer((nc, url) -> {
            Properties connectionProperties = new Properties() ;
            connectionProperties.put(Options.PROP_URL, url);
            String subject = random();
            Thread t = new Thread(new Publisher(nc, subject));
            t.start();
            _testSource(subject, connectionProperties, null);
            t.join();
        });
    }

    static class Publisher implements Runnable {
        final Connection nc;
        final String subject;
        final AtomicInteger count;

        public Publisher(Connection nc, String subject) {
            this.nc = nc;
            this.subject = subject;
            this.count = new AtomicInteger();
        }

        @Override
        public void run() {
            //noinspection InfiniteLoopStatement
            while (true) {
                nc.publish(subject, ("data" + count.incrementAndGet()).getBytes());
                try {
                    //noinspection BusyWait
                    Thread.sleep(200);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static void _testSource(String subject,
                                    Properties connectionProperties,
                                    String connectionPropertiesFile) throws Exception
    {
        final StringPayloadDeserializer deserializer = new StringPayloadDeserializer();
        NatsSourceBuilder<String> builder = new NatsSourceBuilder<String>()
            .subjects(subject)
            .payloadDeserializer(deserializer);

        if (connectionProperties == null) {
            builder.connectionPropertiesFile(connectionPropertiesFile);
        }
        else {
            builder.connectionProperties(connectionProperties);
        }

        NatsSource<String> natsSource = builder.build();
        StreamExecutionEnvironment env = getStreamExecutionEnvironment();
        DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-input");
        ds.sinkTo(new PrintSink<String>());

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
        env.execute("TestSource");
    }
}
