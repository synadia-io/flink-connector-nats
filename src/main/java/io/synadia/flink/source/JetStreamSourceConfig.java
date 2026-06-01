// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Boundedness;

import java.util.Objects;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 *
 * <p>Bundle of source-level configuration shared between {@link JetStreamSource}
 * and {@link io.synadia.flink.source.reader.JetStreamSourceReader}: boundedness,
 * source reader queue capacity, and consumer strategy. Fields are
 * {@code public final} — accessed both within {@code io.synadia.flink.source}
 * and from the {@code source.reader} package.</p>
 */
@Internal
public class JetStreamSourceConfig {
    /** Boundedness of the source, derived from the subject configurations. */
    public final Boundedness boundedness;

    /** Convenience flag: {@code boundedness == Boundedness.BOUNDED}. */
    public final boolean bounded;

    /** Source reader element queue capacity (-1 = let the reader fall back). */
    public final int sourceQueueCapacity;

    /** Consume strategy: Polled (fetch) or Dispatched (push). */
    public final ConsumerStrategy consumerStrategy;

    public JetStreamSourceConfig(Boundedness boundedness,
                                 int sourceQueueCapacity,
                                 ConsumerStrategy consumerStrategy) {
        this.boundedness = boundedness;
        this.bounded = boundedness == Boundedness.BOUNDED;
        this.sourceQueueCapacity = sourceQueueCapacity;
        this.consumerStrategy = consumerStrategy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JetStreamSourceConfig)) return false;
        JetStreamSourceConfig that = (JetStreamSourceConfig) o;
        return boundedness == that.boundedness
            && sourceQueueCapacity == that.sourceQueueCapacity
            && consumerStrategy == that.consumerStrategy;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(boundedness);
        result = 31 * result + sourceQueueCapacity;
        result = 31 * result + Objects.hashCode(consumerStrategy);
        return result;
    }

    @Override
    public String toString() {
        return "JetStreamSourceConfig{" +
            "boundedness=" + boundedness +
            ", sourceQueueCapacity=" + sourceQueueCapacity +
            ", consumerStrategy=" + consumerStrategy +
            '}';
    }
}
