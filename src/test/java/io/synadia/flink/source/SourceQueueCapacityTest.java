// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.synadia.flink.message.Utf8StringSourceConverter;
import io.synadia.flink.source.reader.JetStreamSourceReader;
import io.synadia.flink.source.reader.NatsSourceReader;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Validates that each source reader's internal {@link FutureCompletingBlockingQueue}
 * is sized from the constructor's {@code sourceQueueCapacity} argument, floored
 * at {@link SourceReaderOptions#ELEMENT_QUEUE_CAPACITY}'s default value.
 *
 * <p>Reflects on the private {@code queue} field and reads
 * {@link FutureCompletingBlockingQueue#remainingCapacity()} on a freshly
 * constructed (empty) queue — when empty, {@code remainingCapacity()} equals
 * the configured capacity.</p>
 */
class SourceQueueCapacityTest {

    private static final int FLINK_DEFAULT =
        SourceReaderOptions.ELEMENT_QUEUE_CAPACITY.defaultValue();   // 2

    // ----- NatsSourceReader -----

    @Test
    void natsReader_explicitCapacityIsHonored() throws Exception {
        assertEquals(32,  natsReaderQueueCapacity(32));
        assertEquals(64,  natsReaderQueueCapacity(64));
        assertEquals(500, natsReaderQueueCapacity(500));
    }

    @Test
    void natsReader_belowFlinkDefaultIsFloored() throws Exception {
        assertEquals(FLINK_DEFAULT, natsReaderQueueCapacity(-1));
        assertEquals(FLINK_DEFAULT, natsReaderQueueCapacity(0));
        assertEquals(FLINK_DEFAULT, natsReaderQueueCapacity(1));
        assertEquals(FLINK_DEFAULT, natsReaderQueueCapacity(2));
    }

    @Test
    void natsReader_exactlyFlinkDefaultPlusOne() throws Exception {
        // Smallest value that survives the floor unchanged.
        assertEquals(FLINK_DEFAULT + 1, natsReaderQueueCapacity(FLINK_DEFAULT + 1));
    }

    // ----- JetStreamSourceReader -----

    @Test
    void jetStreamReader_explicitCapacityIsHonored() throws Exception {
        assertEquals(32,  jetStreamReaderQueueCapacity(32));
        assertEquals(64,  jetStreamReaderQueueCapacity(64));
        assertEquals(500, jetStreamReaderQueueCapacity(500));
    }

    @Test
    void jetStreamReader_belowFlinkDefaultIsFloored() throws Exception {
        assertEquals(FLINK_DEFAULT, jetStreamReaderQueueCapacity(-1));
        assertEquals(FLINK_DEFAULT, jetStreamReaderQueueCapacity(0));
        assertEquals(FLINK_DEFAULT, jetStreamReaderQueueCapacity(1));
        assertEquals(FLINK_DEFAULT, jetStreamReaderQueueCapacity(2));
    }

    @Test
    void jetStreamReader_exactlyFlinkDefaultPlusOne() throws Exception {
        assertEquals(FLINK_DEFAULT + 1, jetStreamReaderQueueCapacity(FLINK_DEFAULT + 1));
    }

    // ----- helpers -----

    private static int natsReaderQueueCapacity(int sourceQueueCapacity) throws Exception {
        try (NatsSourceReader<String> reader = new NatsSourceReader<>(
                mock(ConnectionFactory.class),
                new Utf8StringSourceConverter(),
                mock(SourceReaderContext.class),
                sourceQueueCapacity)) {
            return queueCapacity(reader);
        }
    }

    private static int jetStreamReaderQueueCapacity(int sourceQueueCapacity) throws Exception {
        try (JetStreamSourceReader<String> reader = new JetStreamSourceReader<>(
                Boundedness.CONTINUOUS_UNBOUNDED,
                new Utf8StringSourceConverter(),
                mock(ConnectionFactory.class),
                mock(SourceReaderContext.class),
                sourceQueueCapacity)) {
            return queueCapacity(reader);
        }
    }

    private static int queueCapacity(Object reader) throws Exception {
        Field f = reader.getClass().getDeclaredField("queue");
        f.setAccessible(true);
        FutureCompletingBlockingQueue<?> q = (FutureCompletingBlockingQueue<?>) f.get(reader);
        // Queue is empty (freshly constructed), so remainingCapacity == capacity.
        return q.remainingCapacity();
    }
}
