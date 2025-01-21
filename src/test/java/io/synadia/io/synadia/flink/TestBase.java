// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.io.synadia.flink;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.synadia.flink.v0.payload.ByteArrayPayloadSerializer;
import io.synadia.flink.v0.payload.Payload;
import io.synadia.flink.v0.payload.PayloadBytesSerializer;
import io.synadia.flink.v0.payload.StringPayloadSerializer;
import io.synadia.flink.v0.sink.NatsJetStreamSink;
import io.synadia.flink.v0.sink.NatsJetStreamSinkBuilder;
import io.synadia.flink.v0.sink.NatsSink;
import io.synadia.flink.v0.sink.NatsSinkBuilder;
import io.synadia.flink.v0.utils.ConnectionContext;
import io.synadia.flink.v0.utils.ConnectionFactory;
import nats.io.ConsoleOutput;
import nats.io.NatsServerRunner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.logging.Level;

import static io.synadia.flink.v0.utils.ConnectionContext.ACK_BODY_BYTES;

public class TestBase {
    public static final String PLAIN_ASCII = "hello world ascii";
    public static final List<String> UTF8_TEST_STRINGS = new ArrayList<>();
    public static final List<String> WORD_COUNT_JSONS = new ArrayList<>();
    public static final Map<String, Integer> WORD_COUNT_MAP = new HashMap<>();

    static {
        NatsServerRunner.setDefaultOutputSupplier(ConsoleOutput::new);
        quiet();

        UTF8_TEST_STRINGS.addAll(resourceAsLines("utf8-test-strings.txt"));
        WORD_COUNT_JSONS.addAll(resourceAsLines("word-count-jsons.txt"));

        List<String> words = resourceAsLines("words.txt");
        for (String word : words) {
            WORD_COUNT_MAP.merge(word.toLowerCase(), 1, Integer::sum);
        }
    }

    public static void quiet() {
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
    }

    public static StreamExecutionEnvironment getStreamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        return env;
    }

    public static DataStream<String> getPayloadDataStream(StreamExecutionEnvironment env) {
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

    public static void runInJsServer(InServerTest inServerTest) throws Exception {
        runInServer(true, inServerTest);
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
    // ssl utils
    // ----------------------------------------------------------------------------------------------------
    public static String KEYSTORE_PATH = "src/test/resources/keystore.jks";
    public static String TRUSTSTORE_PATH = "src/test/resources/truststore.jks";
    public static String PASSWORD = "password";

    public static Properties defaultConnectionProperties(String url) {
        Properties connectionProperties = new Properties() ;
        connectionProperties.put(Options.PROP_URL, url);
        connectionProperties.put(Options.PROP_ERROR_LISTENER, "io.synadia.io.synadia.flink.NoOpErrorListener");
        return connectionProperties;
    }

    public static Properties addTestSslProperties(Properties props) {
        props = props == null ? new Properties() : props;
        props.setProperty(Options.PROP_KEYSTORE, KEYSTORE_PATH);
        props.setProperty(Options.PROP_KEYSTORE_PASSWORD, PASSWORD);
        props.setProperty(Options.PROP_TRUSTSTORE, TRUSTSTORE_PATH);
        props.setProperty(Options.PROP_TRUSTSTORE_PASSWORD, PASSWORD);
        return props;
    }

    public static NatsSink<String> newNatsStringSink(String subject, Properties connectionProperties, String connectionPropertiesFile) {
        final StringPayloadSerializer serializer = new StringPayloadSerializer();
        NatsSinkBuilder<String> builder = new NatsSinkBuilder<String>()
            .subjects(subject)
            .payloadSerializer(serializer);

        if (connectionProperties == null) {
            builder.connectionPropertiesFile(connectionPropertiesFile);
        }
        else {
            builder.connectionProperties(connectionProperties);
        }
        return builder.build();
    }

    public static NatsSink<Byte[]> newNatsByteArraySink(String subject, Properties connectionProperties, String connectionPropertiesFile) {
        final ByteArrayPayloadSerializer serializer = new ByteArrayPayloadSerializer();
        NatsSinkBuilder<Byte[]> builder = new NatsSinkBuilder<Byte[]>()
            .subjects(subject)
            .payloadSerializer(serializer);

        if (connectionProperties == null) {
            if (connectionPropertiesFile != null) {
                builder.connectionPropertiesFile(connectionPropertiesFile);
            }
        }
        else {
            builder.connectionProperties(connectionProperties);
        }
        return builder.build();
    }

    public static NatsJetStreamSink<String> newNatsJetStreamStringSink(String subject, Properties connectionProperties, String connectionPropertiesFile) {
        final StringPayloadSerializer serializer = new StringPayloadSerializer();
        NatsJetStreamSinkBuilder<String> builder = new NatsJetStreamSinkBuilder<String>()
            .subjects(subject)
            .payloadSerializer(serializer);

        if (connectionProperties == null) {
            builder.connectionPropertiesFile(connectionPropertiesFile);
        }
        else {
            builder.connectionProperties(connectionProperties);
        }
        return builder.build();
    }

    public static NatsJetStreamSink<Payload<byte[]>> newNatsJetStreamPayloadBytesSink(String subject, Properties connectionProperties, String connectionPropertiesFile) {
        final PayloadBytesSerializer serializer = new PayloadBytesSerializer();
        NatsJetStreamSinkBuilder<Payload<byte[]>> builder = new NatsJetStreamSinkBuilder<Payload<byte[]>>()
                .subjects(subject)
                .payloadSerializer(serializer);

        if (connectionProperties == null) {
            builder.connectionPropertiesFile(connectionPropertiesFile);
        }
        else {
            builder.connectionProperties(connectionProperties);
        }
        return builder.build();
    }

    // ----------------------------------------------------------------------------------------------------
    // misc / macro utils
    // ----------------------------------------------------------------------------------------------------

    public static String createTempPropertiesFile(Properties props) throws IOException {
        File f = File.createTempFile("fcn", ".properties");
        BufferedWriter writer = new BufferedWriter(new FileWriter(f));
        for (String key : props.stringPropertyNames()) {
            writer.write(key + "=" + props.getProperty(key) + System.lineSeparator());
        }
        writer.flush();
        writer.close();
        return f.getAbsolutePath();
    }

    public static List<String> resourceAsLines(String fileName) {
        try {
            ClassLoader classLoader = TestBase.class.getClassLoader();
            //noinspection DataFlowIssue
            File file = new File(classLoader.getResource(fileName).getFile());
            return Files.readAllLines(file.toPath());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

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
        System.out.println(sb);
    }

    public static Object javaSerializeDeserializeObject(Serializable inObject) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos);
        objectOutputStream.writeObject(inObject);
        objectOutputStream.flush();
        objectOutputStream.close();
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(bais);
        Object outObject = objectInputStream.readObject();
        objectInputStream.close();
        return outObject;
    }

    // this FlatMapFunction requires members to be serializable, Nats connection is not serializable
    // AckMessageFunction acks messages in a sequence up to a stopAtSeq
    public static class AckMessageFunction implements FlatMapFunction<Payload<byte[]>, Payload<byte[]>> {
        public final ConnectionFactory connectionFactory;
        private int currSeq;
        private final int stopAtSeq;

        public AckMessageFunction(ConnectionFactory connectionFactory, int currSeq, int stopAtSeq) throws IllegalArgumentException {
            this.connectionFactory = connectionFactory;
            if (currSeq < 0 || currSeq > stopAtSeq) {
                throw new IllegalArgumentException("currSeq must be less than stopAtSeq");
            }

            this.currSeq = currSeq;
            this.stopAtSeq = stopAtSeq;
        }

        @Override
        public void flatMap(Payload<byte[]> p, Collector<Payload<byte[]>> out) throws Exception {
            if (currSeq > stopAtSeq) {
                return;
            }

            assert p.replyTo != null;
            ConnectionContext context = connectionFactory.connectContext();

            // introduce random delays to simulate user acking at different intervals ranging from 100-500 ms
            long sleep = (long) (Math.random() * 400) + 100;

            System.out.println(p);
            System.out.println("sleeping for " + sleep + "ms");

            Thread.sleep(sleep);

            context.connection.publish(p.replyTo, ACK_BODY_BYTES);
            currSeq++;
        }
    }
}
