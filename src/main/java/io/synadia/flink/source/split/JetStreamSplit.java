// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.split;

import io.nats.client.Message;
import io.nats.client.support.*;
import io.synadia.flink.source.JetStreamSubjectConfiguration;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

public class JetStreamSplit implements SourceSplit, JsonSerializable {
    private static final Logger LOG = LoggerFactory.getLogger(JetStreamSplit.class);
    public static final String FINISHED = "finished";

    public final AtomicLong lastEmittedStreamSequence;
    public final AtomicLong emittedCount;
    public final AtomicBoolean finished;
    public final JetStreamSubjectConfiguration subjectConfig;

    public JetStreamSplit(JetStreamSubjectConfiguration subjectConfig){
        lastEmittedStreamSequence = new AtomicLong(-1);
        emittedCount = new AtomicLong(0);
        finished = new AtomicBoolean(false);
        this.subjectConfig = subjectConfig;
    }

    public JetStreamSplit(String json) {
        try {
            JsonValue jv = JsonParser.parse(json);
            lastEmittedStreamSequence = new AtomicLong(JsonValueUtils.readLong(jv, LAST_SEQ, -1));
            emittedCount = new AtomicLong(JsonValueUtils.readLong(jv, MSGS, 0));
            finished = new AtomicBoolean(JsonValueUtils.readBoolean(jv, FINISHED, false));
            JsonValue jcConfig = JsonValueUtils.readObject(jv, CONFIG);
            subjectConfig = JetStreamSubjectConfiguration.fromJsonValue(jcConfig);
        }
        catch (JsonParseException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, LAST_SEQ, lastEmittedStreamSequence.get());
        JsonUtils.addField(sb, MSGS, emittedCount.get());
        JsonUtils.addField(sb, FINISHED, finished.get());
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

    public long markEmitted(Message message) {
        long lastSeq = message.metaData().streamSequence();
        LOG.debug("{} setLastEmittedStreamSequence {} --> {}", splitId(), this.lastEmittedStreamSequence.get(), lastEmittedStreamSequence);
        this.lastEmittedStreamSequence.set(lastSeq);
        return emittedCount.incrementAndGet();
    }

    public void setFinished() {
        LOG.debug("{} setFinished", splitId());
        this.finished.set(true);
    }

    @Override
    public String toString() {
        return "JetStreamSplit{" +
            "lastEmittedStreamSequence=" + lastEmittedStreamSequence +
            "emittedCount=" + emittedCount +
            ", finished=" + finished +
            ", subjectConfig=" + subjectConfig +
            '}';
    }
}
