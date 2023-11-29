// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.
package io.synadia.io.synadia.flink;

import static org.junit.jupiter.api.Assertions.assertTrue;
import io.nats.client.api.SequenceInfo;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.source.NatsConsumerConfig;
import io.synadia.flink.source.NatsJetstreamSource;
import io.synadia.flink.source.NatsJetstreamSourceBuilder;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

public class NatsJetstreamSourceTest extends TestBase{

    @Test
    public void testSource() throws Exception {
        String sourceSubject1 = "test";
        String streamName = "test";
        String consumerName = "testconsumer";

        runInServer(true, (nc, url) -> {

            // publish to the source's subjects
            StreamConfiguration stream = new StreamConfiguration.Builder().name(streamName).subjects(sourceSubject1).build();
            nc.jetStreamManagement().addStream(stream);
            nc.jetStream().publish(sourceSubject1, "Hi".getBytes());
            nc.jetStream().publish(sourceSubject1, "Hello".getBytes());

            // --------------------------------------------------------------------------------
            Properties connectionProperties = defaultConnectionProperties(url);
            DeserializationSchema<String> deserializer = new SimpleStringSchema();
            NatsConsumerConfig consumerConfig = new NatsConsumerConfig.Builder().withConsumerName(consumerName).
                    withBatchSize(5).build();
            NatsJetstreamSourceBuilder<String> builder = new NatsJetstreamSourceBuilder<String>()
                    .subjects(sourceSubject1)
                    .payloadDeserializer(deserializer)
                    .consumerConfig(consumerConfig);
            builder.connectionProperties(connectionProperties);

            NatsJetstreamSource<String> natsSource = builder.build();
            StreamExecutionEnvironment env = getStreamExecutionEnvironment();
            DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(),"nats-source-input");
            ds.map(String::toUpperCase);//To Avoid Sink Dependency
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
            env.executeAsync("nats-flink");
            Thread.sleep(5000);
            env.close();
            SequenceInfo sequenceInfo = nc.jetStream().getConsumerContext(sourceSubject1,consumerName).getConsumerInfo().getDelivered();
            assertTrue(sequenceInfo.getStreamSequence()>0);
        });
    }
}
