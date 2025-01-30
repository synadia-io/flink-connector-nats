// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.split;

import io.nats.client.support.*;
import io.synadia.flink.v0.source.ManagedSubjectConfiguration;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.support.ApiConstants.CONFIG;
import static io.nats.client.support.ApiConstants.LAST_SEQ;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

public class ManagedSplit implements SourceSplit, JsonSerializable {

    private final AtomicLong lastEmittedStreamSequence;
    private final ManagedSubjectConfiguration subjectConfig;

    public ManagedSplit(ManagedSubjectConfiguration subjectConfig){
        this.subjectConfig = subjectConfig;
        this.lastEmittedStreamSequence = new AtomicLong(-1);
    }

    public ManagedSplit(String json) {
        try {
            JsonValue jv = JsonParser.parse(json);
            this.lastEmittedStreamSequence = new AtomicLong(JsonValueUtils.readLong(jv, LAST_SEQ, -1));
            JsonValue jcConfig = JsonValueUtils.readObject(jv, CONFIG);
            subjectConfig = new ManagedSubjectConfiguration.Builder().jsonValue(jcConfig).build();
        }
        catch (JsonParseException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, LAST_SEQ, lastEmittedStreamSequence.get());
        JsonUtils.addField(sb, CONFIG, subjectConfig);
        return endJson(sb).toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String splitId() {
        return subjectConfig.getConfigId();
    }

    public long getLastEmittedStreamSequence() {
        return lastEmittedStreamSequence.get();
    }

    public void setLastEmittedStreamSequence(long lastEmittedStreamSequence) {
        this.lastEmittedStreamSequence.set(lastEmittedStreamSequence);
    }

    @Override
    public String toString() {
        return "ManagedSplit{" +
            "splitId='" + subjectConfig.getConfigId() + '\'' +
            ", lastStreamSequence=" + lastEmittedStreamSequence.get() +
            '}';
    }
}
