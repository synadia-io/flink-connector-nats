// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.split;

import io.nats.client.Message;
import io.nats.client.support.*;
import io.synadia.flink.source.JetStreamSubjectConfiguration;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.FlinkRuntimeException;
import org.jspecify.annotations.NonNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.synadia.flink.utils.Constants.*;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class JetStreamSplit implements SourceSplit, JsonSerializable {
    /**
     * The last emitted stream sequence
     */
    public final AtomicLong lastEmittedStreamSequence;

    /**
     * The last emitted message reply to
     */
    public final AtomicReference<String> lastEmittedMessageReplyTo;

    /**
     * The count of emitted messages
     */
    public final AtomicLong emittedCount;

    /**
     * The finished state
     */
    public final AtomicBoolean finished;

    /**
     * The configuration
     */
    public final JetStreamSubjectConfiguration subjectConfig;

    /**
     * Construct a JetStreamSplit
     * @param subjectConfig the subject configuration
     */
    public JetStreamSplit(JetStreamSubjectConfiguration subjectConfig){
        lastEmittedStreamSequence = new AtomicLong(-1);
        lastEmittedMessageReplyTo = new AtomicReference<>();
        emittedCount = new AtomicLong(0);
        finished = new AtomicBoolean(false);
        this.subjectConfig = subjectConfig;
    }

    /**
     * Construct a JetStreamSplit from JSON
     * @param json the json
     */
    public JetStreamSplit(String json) {
        try {
            JsonValue jv = JsonParser.parse(json);
            lastEmittedStreamSequence = new AtomicLong(JsonValueUtils.readLong(jv, LAST_EMITTED_SEQ, -1));
            lastEmittedMessageReplyTo = new AtomicReference<>(JsonValueUtils.readString(jv, LAST_REPLY_TO));
            emittedCount = new AtomicLong(JsonValueUtils.readLong(jv, MESSAGES, 0));
            finished = new AtomicBoolean(JsonValueUtils.readBoolean(jv, FINISHED, false));
            JsonValue jcConfig = JsonValueUtils.readObject(jv, SUBJECT_CONFIG);
            subjectConfig = JetStreamSubjectConfiguration.fromJsonValue(jcConfig);
        }
        catch (JsonParseException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    @NonNull public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, LAST_EMITTED_SEQ, lastEmittedStreamSequence.get());
        JsonUtils.addField(sb, LAST_REPLY_TO, lastEmittedMessageReplyTo.get());
        JsonUtils.addField(sb, MESSAGES, emittedCount.get());
        JsonUtils.addField(sb, FINISHED, finished.get());
        JsonUtils.addField(sb, SUBJECT_CONFIG, subjectConfig);
        return endJson(sb).toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String splitId() {
        return subjectConfig.id;
    }

    /**
     * mark a message as emitted
     * @param message the message
     * @return the total emitted count
     */
    public long markEmitted(Message message) {
        this.lastEmittedStreamSequence.set(message.metaData().streamSequence());
        this.lastEmittedMessageReplyTo.set(message.getReplyTo());
        return emittedCount.incrementAndGet();
    }

    /**
     * Set the split as finished
     */
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
