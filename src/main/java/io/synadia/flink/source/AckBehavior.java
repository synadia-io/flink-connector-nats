// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.api.AckPolicy;

import java.util.HashMap;
import java.util.Map;

/**
 * Enum for allowable ack behaviors
 */
public enum AckBehavior {

    /**
     * Ordered consumer used, no acks, messages are not acknowledged
     */
    NoAck("NoAck", AckPolicy.None, true),

    /**
     * Non-ordered consumer used (allows for durability settings), no acks, messages are not acknowledged
     */
    NoAckUnordered("NoAckUnordered", AckPolicy.None, true),

    /**
     * Consumer uses AckPolicy.All. Messages are tracked as they are sourced
     * and the last one is acked at checkpoint
     */
    AckAll("AckAll", AckPolicy.All, false),

    /**
     * Consumer uses AckPolicy.All but the source does not ack
     * at the checkpoint, leaving acking up to the user.
     * If messages are not acked in time, they will be redelivered to the source.
     */
    AllButDoNotAck("AllButDoNotAck", AckPolicy.All, false),

    /**
     * Consumer uses AckPolicy.Explicit but the source does not ack
     * at the checkpoint, leaving acking up to the user.
     * If messages are not acked in time, they will be redelivered to the source.
     */
    ExplicitButDoNotAck("ExplicitButDoNotAck", AckPolicy.Explicit, false);

    /**
     * The behavior name and json value
     */
    public final String behavior;

    /**
     * The AckPolicy
     */
    public final AckPolicy ackPolicy;

    /**
     * Whether it represents a policy that does not ack
     */
    public final boolean isNoAck;

    private static final Map<String, AckBehavior> strEnumHash = new HashMap<>();

    AckBehavior(String behavior, AckPolicy ackPolicy, boolean isNoAck) {
        this.behavior = behavior;
        this.ackPolicy = ackPolicy;
        this.isNoAck = isNoAck;
    }

    public String toString() {
        return this.behavior;
    }

    /**
     * Get the AckBehavior from a string value
     * @param value the case-insensitive string value
     * @return the AckBehavior or null if a match cannot be found
     */
    public static AckBehavior get(String value) {
        return value == null ? null : strEnumHash.get(value.toLowerCase());
    }

    static {
        for(AckBehavior env : values()) {
            strEnumHash.put(env.toString().toLowerCase(), env);
        }
    }
}

