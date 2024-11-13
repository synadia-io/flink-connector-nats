// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.utils;

import io.nats.client.NUID;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

public abstract class MiscUtils {
    /**
     * Current version of the library
     */
    public static final String CLIENT_VERSION;

    static {
        String cv;
        try { cv = MiscUtils.class.getPackage().getImplementationVersion(); }
        catch (Exception ignore) { cv = null; }
        if (cv == null) {
            try {
                List<String> lines = Files.readAllLines(new File("build.gradle").toPath());
                for (String l : lines) {
                    if (l.startsWith("def jarVersion")) {
                        int at = l.indexOf('"');
                        int lat = l.lastIndexOf('"');
                        cv = l.substring(at + 1, lat) + ".dev";
                        break;
                    }
                }
            }
            catch (Exception ignore) {}
        }
        CLIENT_VERSION = cv == null ? "development" : cv;
    }

    public static String generateId() {
        return NUID.nextGlobal().substring(0, 4);
    }

    public static String generatePrefixedId(String prefix) {
        String temp = NUID.nextGlobal();
        return prefix + "-" + temp.substring(temp.length() - 5);
    }
}
