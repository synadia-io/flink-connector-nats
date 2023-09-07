package io.synadia;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import nats.io.ConsoleOutput;
import nats.io.NatsServerRunner;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;

public class TestBase {
    static {
        NatsServerRunner.setDefaultOutputSupplier(ConsoleOutput::new);
        quiet();
    }

    public static void quiet() {
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
    }


    public static StreamExecutionEnvironment getStreamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        return env;
    }

    public static DataStream<String> getStringDataStream(StreamExecutionEnvironment env) {
        FileSource.FileSourceBuilder<String> builder =
            FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("src/test/resources/words.txt")
            );

        return env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "file-input");
    }

    // ----------------------------------------------------------------------------------------------------
    // JetStream support
    // ----------------------------------------------------------------------------------------------------
    public static class TestStream {
        public final String stream;
        public final String subject;
        public final String[] subjects;
        public final StreamInfo si;

        public TestStream(JetStreamManagement jsm) throws JetStreamApiException, IOException {
            this(jsm, subject());
        }

        public TestStream(JetStreamManagement jsm, String... subjects) throws JetStreamApiException, IOException {

            stream = stream();
            this.subjects = subjects;
            subject = subjects[0];

            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.Memory)
                .subjects(subjects).build();
            si = jsm.addStream(sc);
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // runners
    // ----------------------------------------------------------------------------------------------------
    public interface InServerTest {
        void test(Connection nc, String url) throws Exception;
    }

    public static void runInServer(InServerTest inServerTest) throws Exception {
        runInServer(false, inServerTest);
    }

    public static void runInServer(boolean jetstream, InServerTest inServerTest) throws Exception {
        try (NatsServerRunner runner = new NatsServerRunner(false, jetstream);
             Connection nc = Nats.connect(runner.getURI()))
        {
            try {
                inServerTest.test(nc, getUrl(nc));
            }
            finally {
                if (jetstream) {
                    cleanupJs(nc);
                }
            }
        }
    }

    public static void runInExternalServer(InServerTest inServerTest) throws Exception {
        runInExternalServer(Options.DEFAULT_URL, false, inServerTest);
    }

    public static void runInExternalServer(boolean jetstream, InServerTest inServerTest) throws Exception {
        runInExternalServer(Options.DEFAULT_URL, jetstream, inServerTest);
    }

    public static void runInExternalServer(String url, boolean jetstream, InServerTest inServerTest) throws Exception {
        try (Connection nc = Nats.connect(url)) {
            try {
                inServerTest.test(nc, url);
            }
            finally {
                if (jetstream) {
                    cleanupJs(nc);
                }
            }
        }
    }

    private static String getUrl(Connection nc) {
        return "nats://localhost:" + nc.getServerInfo().getPort();
    }

    private static void cleanupJs(Connection c)
    {
        try {
            JetStreamManagement jsm = c.jetStreamManagement();
            List<String> streams = jsm.getStreamNames();
            for (String s : streams)
            {
                jsm.deleteStream(s);
            }
        } catch (Exception ignore) {}
    }

    public static String random() {
        return NUID.nextGlobalSequence();
    }

    public static String random(String prefix) {
        return prefix + "-" + NUID.nextGlobalSequence();
    }

    public static String stream() {
        return random("stream");
    }

    public static String subject() {
        return random("subject");
    }

    public static String name() {
        return random("name");
    }

    // ----------------------------------------------------------------------------------------------------
    // utils / macro utils
    // ----------------------------------------------------------------------------------------------------
    public static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { /* ignored */ }
    }

    public static void debug(Object... debug) {
        StringBuilder sb = new StringBuilder();
        sb.append(System.currentTimeMillis());
        sb.append(" [");
        sb.append(Thread.currentThread().getName());
        sb.append(",");
        sb.append(Thread.currentThread().getPriority());
        sb.append("] ");
        boolean flag = true;
        for (Object o : debug) {
            if (flag) {
                flag = false;
            }
            else {
                sb.append(" | ");
            }
            sb.append(o);
        }
        System.out.println(sb.toString());
    }
}
