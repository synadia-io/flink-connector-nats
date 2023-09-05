// Copyright (c) 2023 Synadia Communications Inc.  All Rights Reserved.

package synadia.io;

import io.nats.client.JetStreamManagement;
import io.nats.client.Options;
import io.nats.client.api.ServerInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class BasicTests extends TestBase {

    @Test
    public void testBasicFlink() throws Exception {
        StreamExecutionEnvironment env = getStreamExecutionEnvironment();
        DataStream<String> text = getStringDataStream(env);

        text.addSink(new SinkFunction<>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println("SINK: " + value);
//                SinkFunction.super.invoke(value, context);
            }
        });

        env.execute("BasicFlink");
    }

    private static DataStream<String> getStringDataStream(StreamExecutionEnvironment env) {
        FileSource.FileSourceBuilder<String> builder =
            FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("src/test/resources/word-count.txt")
            );

        DataStream<String> text = env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "file-input");
        return text;
    }

    private static StreamExecutionEnvironment getStreamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        return env;
    }


    @Test
    public void testInServer() throws Exception {
        runInExternalServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestStream ts = new TestStream(jsm, "foo");
            ServerInfo si = nc.getServerInfo();
            String url = "nats://localhost:" + si.getPort();
            System.out.println(url + " " + ts.stream + " " + ts.subject);
            Properties props = new Properties();
            props.put(Options.PROP_URL, url);

            StreamExecutionEnvironment env = getStreamExecutionEnvironment();

            final NatsStringPayloadSerializer serializer = new NatsStringPayloadSerializer();
            final NatsSink<String> natsSink = new NatsSink<>(ts.subject, props, serializer);
            final SinkWriter<String> writer = natsSink.createWriter(null);

            DataStream<String> text = getStringDataStream(env);

            text.addSink(new SinkFunction<>() {
                @Override
                public void invoke(String value, Context context) throws Exception {
                    System.out.println("SINK: " + value);
                    writer.write(value, null);
                }
            });

            writer.close();
            env.execute("InServerTest");
        });
    }
}
