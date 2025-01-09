package io.synadia.io.synadia.flink.v0.testutils;

import io.nats.client.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NatsSourceTestEnv {
    private static final Logger LOG = LoggerFactory.getLogger(NatsSourceTestEnv.class);
    
    protected static Connection natsConnection;
    protected static JetStreamManagement jsm;
    public static final int NUM_MESSAGES_PER_SUBJECT = 10;

    @BeforeAll
    public static void prepare() throws Exception {
        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    Starting NatsSourceTestEnv ");
        LOG.info("-------------------------------------------------------------------------");

        natsConnection = Nats.connect(Options.DEFAULT_URL);
        jsm = natsConnection.jetStreamManagement();
    }


    protected static void publishTestMessages(String subject, int numMessages) throws Exception {
        JetStream js = natsConnection.jetStream();
        for (int i = 0; i < numMessages; i++) {
            js.publish(subject, String.valueOf(i).getBytes());
        }
    }


    @AfterAll
    public static void shutDownServices() throws Exception {
        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    Shutting down NatsSourceTestEnv ");
        LOG.info("-------------------------------------------------------------------------");

        if (natsConnection != null) {
            natsConnection.close();
        }
    }
} 