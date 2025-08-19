// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import nats.io.ConsoleOutput;
import nats.io.NatsServerRunner;

import java.io.File;
import java.nio.file.Files;
import java.util.logging.Level;

public class TestServerContext {
    static {
        NatsServerRunner.DEFAULT_RUN_CHECK_TRIES = 0; // checking myself
        NatsServerRunner.setDefaultOutputSupplier(ConsoleOutput::new);
        quiet();
    }

    public final NatsServerRunner runner;
    public final Connection nc;
    public final String url;
    public final JetStreamManagement jsm;
    public final JetStream js;

    public TestServerContext() throws Exception {
        String[] configInserts = new String[3];
        String dir = Files.createTempDirectory("fcj").toString();
        if (File.separatorChar == '\\') {
            dir = dir.replace("\\", "\\\\").replace("/", "\\\\");
        }
        else {
            dir = dir.replace("\\", "/");
        }
        configInserts[0] = "jetstream {";
        configInserts[1] = "    store_dir=" + dir;
        configInserts[2] = "}";

        runner = new NatsServerRunner(-1, false, true, null, configInserts, null);
        nc = Nats.connect(runner.getURI());
        int retries = 100; // 100 * 50 == 5000 5 seconds
        while (retries > 0) {
            if (nc.getStatus().equals(Connection.Status.CONNECTED)) {
                break;
            }
            Thread.sleep(50);
            retries--;
        }
        if (retries == 0) {
            runner.close();
            throw new IllegalStateException("Could not connect to nats server");
        }

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
