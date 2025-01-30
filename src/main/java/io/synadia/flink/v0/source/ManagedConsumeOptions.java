// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source;

import io.nats.client.BaseConsumeOptions;
import io.nats.client.ConsumeOptions;
import io.nats.client.FetchConsumeOptions;
import io.nats.client.support.SerializableConsumeOptions;
import io.nats.client.support.SerializableFetchConsumeOptions;
import org.apache.flink.api.connector.source.Boundedness;

import java.io.Serializable;

public class ManagedConsumeOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final SerializableConsumeOptions unboundedOptions;
    private final SerializableFetchConsumeOptions boundedOptions;
    private final Boundedness boundedness;

    public ManagedConsumeOptions(Boundedness boundedness) {
        this.boundedness = boundedness;
        if (boundedness == Boundedness.CONTINUOUS_UNBOUNDED) {
            this.unboundedOptions = new SerializableConsumeOptions(ConsumeOptions.DEFAULT_CONSUME_OPTIONS);
            this.boundedOptions = null;
        }
        else {
            this.unboundedOptions = null;
            this.boundedOptions = new SerializableFetchConsumeOptions(FetchConsumeOptions.DEFAULT_FETCH_OPTIONS);
        }
    }

    public ManagedConsumeOptions(ConsumeOptions unboundedOptions) {
        boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        this.unboundedOptions = new SerializableConsumeOptions(unboundedOptions);
        this.boundedOptions = null;
    }

    public ManagedConsumeOptions(FetchConsumeOptions boundedOptions) {
        boundedness = Boundedness.BOUNDED;
        this.unboundedOptions = null;
        this.boundedOptions = new SerializableFetchConsumeOptions(boundedOptions);
    }

    public ConsumeOptions getUnboundedOptions() {
        return unboundedOptions == null ? null : unboundedOptions.getConsumeOptions();
    }

    public FetchConsumeOptions getBoundedOptions() {
        return boundedOptions == null ? null : boundedOptions.getFetchConsumeOptions();
    }

    public Boundedness getBoundedness() {
        return boundedness;
    }

    @SuppressWarnings("DataFlowIssue")
    private BaseConsumeOptions options() {
        return boundedness == Boundedness.CONTINUOUS_UNBOUNDED
            ? unboundedOptions.getConsumeOptions()
            : boundedOptions.getFetchConsumeOptions();
    }

    @SuppressWarnings("DataFlowIssue")
    public int getMessages() {
        return boundedness == Boundedness.CONTINUOUS_UNBOUNDED
            ? unboundedOptions.getConsumeOptions().getBatchSize()
            : boundedOptions.getFetchConsumeOptions().getMaxMessages();
    }

    @SuppressWarnings("DataFlowIssue")
    public long getBytes() {
        return boundedness == Boundedness.CONTINUOUS_UNBOUNDED
            ? unboundedOptions.getConsumeOptions().getBatchBytes()
            : boundedOptions.getFetchConsumeOptions().getMaxBytes();
    }

    public long getExpiresInMillis() {
        return options().getExpiresInMillis();
    }

    public long getIdleHeartbeat() {
        return options().getIdleHeartbeat();
    }

    public int getThresholdPercent() {
        return options().getThresholdPercent();
    }

    public boolean isNoWait() {
        return options().isNoWait();
    }
}
