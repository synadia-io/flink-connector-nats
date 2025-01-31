// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.utils;

import io.nats.client.NUID;

import java.io.File;
import java.nio.file.Files;
import java.util.Collection;
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
        return new NUID().next().substring(0, 4).toLowerCase();
    }

    public static String generatePrefixedId(String prefix) {
        return prefix + "-" + generateId();
    }

    public static String encodedConsumerName(String prefix, String subject) {
        StringBuilder sb = new StringBuilder(prefix).append("--");
        int len = subject.length();
        for (int x = 0; x < len; x++) {
            char c = subject.charAt(x);
            if (c == '.' || c == '>' || c == '*' | c < 33) {
                sb.append("-");
            }
            else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    public static boolean provided(String s) {
        return s != null && !s.isEmpty();
    }

    public static boolean notProvided(String s) {
        return s == null || s.isEmpty();
    }

    public static boolean provided(Collection<?> c) {
        return c != null && !c.isEmpty();
    }

    public static boolean notProvided(Collection<?> c) {
        return c == null || c.isEmpty();
    }
}
