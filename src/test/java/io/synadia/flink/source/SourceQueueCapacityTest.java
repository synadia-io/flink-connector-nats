// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.synadia.flink.message.Utf8StringSourceConverter;
import io.synadia.flink.source.reader.JetStreamSourceReader;
import io.synadia.flink.source.reader.NatsSourceReader;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Validates that each source reader's internal {@link FutureCompletingBlockingQueue}
 * is sized from the constructor's {@code sourceQueueCapacity} argument, floored
 * by {@code MiscUtils.figureCapacity} — which uses
 * {@link SourceReaderOptions#ELEMENT_QUEUE_CAPACITY} from the
 * {@link SourceReaderContext}'s {@link Configuration} when set, falling back
 * to the option's compile-time default ({@code defaultValue()}) when not.
 *
 * <p>Reflects on the private {@code queue} field and reads
 * {@link FutureCompletingBlockingQueue#remainingCapacity()} on a freshly
 * constructed (empty) queue — when empty, {@code remainingCapacity()} equals
 * the configured capacity.</p>
 */
class SourceQueueCapacityTest {

    private static final int FLINK_DEFAULT =
        SourceReaderOptions.ELEMENT_QUEUE_CAPACITY.defaultValue();   // 2

    // ----- NatsSourceReader: empty config (floor = Flink default) -----

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

    // ----- JetStreamSourceReader: empty config (floor = Flink default) -----

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

    // ----- Configuration-supplied ELEMENT_QUEUE_CAPACITY raises the floor -----

    @Test
    void natsReader_configurationFloorIsHonored() throws Exception {
        Configuration conf = new Configuration();
        conf.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 50);

        // Anything at or below 50 is floored at 50.
        assertEquals(50, natsReaderQueueCapacity(-1, conf));
        assertEquals(50, natsReaderQueueCapacity(0,  conf));
        assertEquals(50, natsReaderQueueCapacity(25, conf));
        assertEquals(50, natsReaderQueueCapacity(50, conf));
        // Above the configured floor, the explicit value wins.
        assertEquals(75, natsReaderQueueCapacity(75, conf));
    }

    @Test
    void jetStreamReader_configurationFloorIsHonored() throws Exception {
        Configuration conf = new Configuration();
        conf.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 50);

        assertEquals(50, jetStreamReaderQueueCapacity(-1, conf));
        assertEquals(50, jetStreamReaderQueueCapacity(0,  conf));
        assertEquals(50, jetStreamReaderQueueCapacity(25, conf));
        assertEquals(50, jetStreamReaderQueueCapacity(50, conf));
        assertEquals(75, jetStreamReaderQueueCapacity(75, conf));
    }

    // ----- helpers -----

    private static int natsReaderQueueCapacity(int sourceQueueCapacity) throws Exception {
        return natsReaderQueueCapacity(sourceQueueCapacity, new Configuration());
    }

    private static int natsReaderQueueCapacity(int sourceQueueCapacity, Configuration conf) throws Exception {
        try (NatsSourceReader<String> reader = new NatsSourceReader<>(
                mock(ConnectionFactory.class),
                new Utf8StringSourceConverter(),
                contextWith(conf),
                sourceQueueCapacity)) {
            return queueCapacity(reader);
        }
    }

    private static int jetStreamReaderQueueCapacity(int sourceQueueCapacity) throws Exception {
        return jetStreamReaderQueueCapacity(sourceQueueCapacity, new Configuration());
    }

    private static int jetStreamReaderQueueCapacity(int sourceQueueCapacity, Configuration conf) throws Exception {
        try (JetStreamSourceReader<String> reader = new JetStreamSourceReader<>(
                Boundedness.CONTINUOUS_UNBOUNDED,
                new Utf8StringSourceConverter(),
                mock(ConnectionFactory.class),
                contextWith(conf),
                sourceQueueCapacity)) {
            return queueCapacity(reader);
        }
    }

    private static SourceReaderContext contextWith(Configuration conf) {
        SourceReaderContext ctx = mock(SourceReaderContext.class);
        when(ctx.getConfiguration()).thenReturn(conf);
        return ctx;
    }

    private static int queueCapacity(Object reader) throws Exception {
        Field f = reader.getClass().getDeclaredField("queue");
        f.setAccessible(true);
        FutureCompletingBlockingQueue<?> q = (FutureCompletingBlockingQueue<?>) f.get(reader);
        // Queue is empty (freshly constructed), so remainingCapacity == capacity.
        return q.remainingCapacity();
    }
}
