// Copyright (c) 2023 Synadia Communications Inc.  All Rights Reserved.

package synadia.io;

import io.nats.client.JetStreamManagement;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

public class BasicTests extends TestBase {

    @Test
    public void testBasicFlink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        FileSource.FileSourceBuilder<String> builder =
            FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("src/test/resources/word-count.txt")
            );

        DataStream<String> text = env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "file-input");

        text.addSink(new SinkFunction<>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println("SINK: " + value);
                SinkFunction.super.invoke(value, context);
            }

            @Override
            public void finish() throws Exception {
                SinkFunction.super.finish();
            }
        });

        env.execute("BasicTests");
    }


    @Test
    public void testInServer() throws Exception {
        runInServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestStream ts = new TestStream(jsm);
            System.out.println(ts.si.getJv());
        });
    }
}
