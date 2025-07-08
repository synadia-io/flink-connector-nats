// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import java.util.HashMap;
import java.util.Map;

public enum AckBehavior {
    NoAck("NoAck"),
    AckAll("AckAll"),
    AllButDoNotAck("AllButDoNotAck"),
    ExplicitButDoNotAck("ExplicitButDoNotAck");

    private final String behavior;
    private static final Map<String, AckBehavior> strEnumHash = new HashMap();

    AckBehavior(String p) {
        this.behavior = p;
    }

    public String toString() {
        return this.behavior;
    }

    public static AckBehavior get(String value) {
        return strEnumHash.get(value);
    }

    static {
        for(AckBehavior env : values()) {
            strEnumHash.put(env.toString(), env);
        }
    }
}
