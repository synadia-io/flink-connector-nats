// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import java.util.HashMap;
import java.util.Map;

/**
 * Enum for strategy to use when for source consuming
 */
public enum ConsumerStrategy {

    /**
     * Use a Fetch consumer and poll as needed
     */
    Polled("Polled"),

    /**
     * Use an endless consume where messages are dispatched
     */
    Dispatched("Dispatched");

    /**
     * The strategy name and JSON key
     */
    public final String key;

    private static final Map<String, ConsumerStrategy> strEnumHash = new HashMap<>();

    ConsumerStrategy(String key) {
        this.key = key;
    }

    public String toString() {
        return this.key;
    }

    /**
     * Get the ConsumerStrategy from a string value
     * @param value the case-insensitive string value
     * @return the ConsumerStrategy or null if a match cannot be found
     */
    public static ConsumerStrategy get(String value) {
        return value == null ? null : strEnumHash.get(value.toLowerCase());
    }

    static {
        for(ConsumerStrategy env : values()) {
            strEnumHash.put(env.toString().toLowerCase(), env);
        }
    }
}

