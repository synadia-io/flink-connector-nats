// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import io.nats.client.NUID;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public abstract class Utils {

    /**
     * Create and load a properties object from a file.
     * @param propertiesFilePath a resolvable path to a file from the location the application is running, either relative or absolute
     * @return the properties object loaded from the file
     */
    public static Properties loadPropertiesFromFile(String propertiesFilePath) throws IOException {
        Properties properties = new Properties();
        properties.load(Files.newInputStream(Paths.get(propertiesFilePath)));
        return properties;
    }

    /**
     * Function to generate a unique id.
     * @return an id
     */
    public static String generateId() {
        return new NUID().next();
    }

    /**
     * Execute a jitter sleep
     * @param minConnectionJitter the minimum jitter
     * @param maxConnectionJitter the maximum jitter
     */
    public static void jitter(long minConnectionJitter, long maxConnectionJitter) {
        long sleep;
        if (maxConnectionJitter <= minConnectionJitter) {
            if (minConnectionJitter < 1) {
                return;
            }
            sleep = ThreadLocalRandom.current().nextLong(minConnectionJitter);
        }
        else {
            sleep = ThreadLocalRandom.current().nextLong(minConnectionJitter, maxConnectionJitter);
        }
        try {
            Thread.sleep(sleep);
        }
        catch (InterruptedException e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
