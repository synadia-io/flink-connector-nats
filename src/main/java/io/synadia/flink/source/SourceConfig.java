// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Boundedness;

import java.util.Objects;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 *
 * <p>Bundle of source-level configuration shared between the source classes
 * ({@link NatsSource}, {@link JetStreamSource}) and their readers. Fields are
 * {@code public final} — accessed both within {@code io.synadia.flink.source}
 * and from the {@code source.reader} package.</p>
 */
@Internal
public class SourceConfig {
    /** Boundedness of the source. */
    public final Boundedness boundedness;

    /** Convenience flag: {@code boundedness == Boundedness.BOUNDED}. */
    public final boolean bounded;

    /**
     * Element queue capacity for the source reader. For {@link JetStreamSource}
     * this is computed by {@link JetStreamSourceBuilder#build()} from the
     * subjects' consume options; for {@link NatsSource} it's a user-tunable
     * knob on {@link NatsSourceBuilder} (defaulting to
     * {@link NatsSourceBuilder#DEFAULT_SOURCE_QUEUE_CAPACITY}).
     */
    public final int sourceQueueCapacity;

    public SourceConfig(Boundedness boundedness, int sourceQueueCapacity) {
        this.boundedness = boundedness;
        this.bounded = boundedness == Boundedness.BOUNDED;
        this.sourceQueueCapacity = sourceQueueCapacity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SourceConfig)) return false;
        SourceConfig that = (SourceConfig) o;
        return boundedness == that.boundedness
            && sourceQueueCapacity == that.sourceQueueCapacity;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(boundedness);
        result = 31 * result + sourceQueueCapacity;
        return result;
    }

    @Override
    public String toString() {
        return "SourceConfig{" +
            "boundedness=" + boundedness +
            ", sourceQueueCapacity=" + sourceQueueCapacity +
            '}';
    }
}
