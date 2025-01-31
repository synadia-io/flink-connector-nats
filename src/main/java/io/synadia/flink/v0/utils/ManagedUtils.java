// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.utils;

import org.apache.flink.api.connector.source.Boundedness;

import java.time.ZonedDateTime;

public abstract class ManagedUtils {

    public static final String SEP = "--";
    public static final String NULL_SEGMENT = "na";
    public static final String CN_BOUNDED = "bnd";
    public static final String CN_UNBOUNDED = "unbnd";

    private static StringBuilder nameSafeEncoding(StringBuilder sb, String name) {
        int len = name.length();
        for (int x = 0; x < len; x++) {
            char c = name.charAt(x);
            if (c == '.' || c == '>' || c == '*' | c < 33) {
                sb.append("-");
            }
            else {
                sb.append(c);
            }
        }
        return sb;
    }

    public static String nameSafeEncoding(String name) {
        return nameSafeEncoding(new StringBuilder(), name).toString();
    }

    public static String toConsumerName(String prefix, Boundedness boundedness, String subject) {
        StringBuilder sb = new StringBuilder(prefix).append(SEP);
        nameSafeEncoding(sb, subject);
        sb.append(SEP).append(boundedness == Boundedness.BOUNDED ? CN_BOUNDED : CN_UNBOUNDED);
        return sb.toString();
    }

    public static String toSplitId(Object... parts) {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (Object o : parts) {
            if (first) {
                first = false;
            }
            else {
                sb.append(SEP);
            }
            if (o == null) {
                sb.append(NULL_SEGMENT);
            }
            else if (o instanceof ZonedDateTime) {
                ZonedDateTime zdt = (ZonedDateTime)o;
                sb.append(zdt.toEpochSecond());
            }
            else {
                sb.append(o);
            }
        }
        return Integer.toHexString(sb.toString().hashCode()).toUpperCase();
    }
}
