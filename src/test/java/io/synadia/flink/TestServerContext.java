// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import nats.io.ConsoleOutput;
import nats.io.NatsServerRunner;

import java.util.logging.Level;

public class TestServerContext {
    static {
        NatsServerRunner.setDefaultOutputSupplier(ConsoleOutput::new);
        quiet();
    }

    public final NatsServerRunner runner;
    public final Connection nc;
    public final String url;
    public final JetStreamManagement jsm;
    public final JetStream js;

    public TestServerContext() throws Exception {
        runner = new NatsServerRunner(false, true);
        nc = Nats.connect(runner.getURI());
        url = TestBase.getUrl(nc);
        jsm = nc.jetStreamManagement();
        js = nc.jetStream();
    }

    public void shutdown() {
        try {
            nc.close();
        }
        catch (Exception ignore) {}
        try {
            runner.close();
        }
        catch (Exception ignore) {}
    }

    public static void quiet() {
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
    }
}
