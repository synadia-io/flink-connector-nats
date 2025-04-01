// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.split;

import io.nats.client.Message;
import io.nats.client.support.*;
import io.synadia.flink.source.JetStreamSubjectConfiguration;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.ApiConstants.CONFIG;
import static io.nats.client.support.ApiConstants.MSGS;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

public class JetStreamSplit implements SourceSplit, JsonSerializable {
    public static final String FINISHED = "finished";
    public static final String LAST_REPLY_TO = "last_reply_to";
    public static final String LAST_EMITTED_SEQ = "last_emitted_seq";

    public final AtomicReference<String> lastEmittedMessageReplyTo;
    public final AtomicLong lastEmittedStreamSequence;
    public final AtomicLong emittedCount;
    public final AtomicBoolean finished;
    public final JetStreamSubjectConfiguration subjectConfig;

    public JetStreamSplit(JetStreamSubjectConfiguration subjectConfig){
        lastEmittedMessageReplyTo = new AtomicReference<>();
        lastEmittedStreamSequence = new AtomicLong(-1);
        emittedCount = new AtomicLong(0);
        finished = new AtomicBoolean(false);
        this.subjectConfig = subjectConfig;
    }

    public JetStreamSplit(String json) {
        try {
            JsonValue jv = JsonParser.parse(json);
            lastEmittedMessageReplyTo = new AtomicReference<>(JsonValueUtils.readString(jv, LAST_REPLY_TO));
            lastEmittedStreamSequence = new AtomicLong(JsonValueUtils.readLong(jv, LAST_EMITTED_SEQ, -1));
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
        JsonUtils.addField(sb, LAST_REPLY_TO, lastEmittedMessageReplyTo.get());
        JsonUtils.addField(sb, LAST_EMITTED_SEQ, lastEmittedStreamSequence.get());
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
        return subjectConfig.configId;
    }

    public long markEmitted(Message message) {
        this.lastEmittedMessageReplyTo.set(message.getReplyTo());
        this.lastEmittedStreamSequence.set(message.metaData().streamSequence());
        return emittedCount.incrementAndGet();
    }

    public void setFinished() {
        this.finished.set(true);
    }

    @Override
    public String toString() {
        return "JetStreamSplit{" +
            "subject=" + subjectConfig.subject +
            ", lastEmittedStreamSequence=" + lastEmittedStreamSequence +
            ", emittedCount=" + emittedCount +
            ", finished=" + finished +
            '}';
    }
}
