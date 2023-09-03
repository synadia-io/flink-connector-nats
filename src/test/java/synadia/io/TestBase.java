package synadia.io;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import nats.io.ConsoleOutput;
import nats.io.NatsServerRunner;

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

    public static class TestStream {
        public final String stream;
        public final String subject;
        public final String[] subjects;
        public StreamInfo si;

        public TestStream(JetStreamManagement jsm) throws JetStreamApiException, IOException {
            stream = stream();
            subjects = new String[]{subject()};
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
        void test(Connection nc) throws Exception;
    }

    public static void runInServer(InServerTest inServerTest) throws Exception {
        try (NatsServerRunner runner = new NatsServerRunner(false, true);
             Connection nc = Nats.connect(runner.getURI()))
        {
            try {
                inServerTest.test(nc);
            }
            finally {
                cleanupJs(nc);
            }
        }
    }

    public static void runInExternalServer(InServerTest inServerTest) throws Exception {
        runInExternalServer(Options.DEFAULT_URL, inServerTest);
    }

    public static void runInExternalServer(String url, InServerTest inServerTest) throws Exception {
        try (Connection nc = Nats.connect(url)) {
            inServerTest.test(nc);
        }
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


    public static String variant(String prefix) {
        return prefix + "-" + NUID.nextGlobalSequence();
    }

    public static String stream() {
        return variant("stream");
    }

    public static String subject() {
        return variant("subject");
    }

    public static String name() {
        return variant("name");
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
