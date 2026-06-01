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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Validates each source reader's element queue sizing under
 * "explicit-wins" semantics ({@code MiscUtils.figureCapacity}):
 *
 * <ul>
 *   <li><b>Explicit:</b> any {@code sourceQueueCapacity} other than {@code -1}
 *       is used verbatim, taking precedence over any value set in the reader
 *       context's {@link Configuration}. (The builder normalizes user input
 *       below the compile-time default to {@code -1} before it reaches the
 *       reader.)</li>
 *   <li><b>Unset:</b> the {@code -1} sentinel falls back to the
 *       {@code Configuration} value if present, or to
 *       {@link SourceReaderOptions#ELEMENT_QUEUE_CAPACITY}'s compile-time
 *       default otherwise.</li>
 * </ul>
 *
 * <p>Uses each reader's public {@code getQueueCapacity()} accessor.</p>
 */
class SourceQueueCapacityTest {

    private static final int FLINK_DEFAULT =
        SourceReaderOptions.ELEMENT_QUEUE_CAPACITY.defaultValue();   // 2

    // ----- Explicit value (>= FLINK_DEFAULT) is honored, no Configuration set -----

    @Test
    void natsReader_explicitCapacityIsHonored() throws Exception {
        assertEquals(FLINK_DEFAULT, natsReaderQueueCapacity(FLINK_DEFAULT)); // exact min
        assertEquals(32,  natsReaderQueueCapacity(32));
        assertEquals(64,  natsReaderQueueCapacity(64));
        assertEquals(500, natsReaderQueueCapacity(500));
    }

    @Test
    void jetStreamReader_explicitCapacityIsHonored() throws Exception {
        assertEquals(FLINK_DEFAULT, jetStreamReaderQueueCapacity(FLINK_DEFAULT));
        assertEquals(32,  jetStreamReaderQueueCapacity(32));
        assertEquals(64,  jetStreamReaderQueueCapacity(64));
        assertEquals(500, jetStreamReaderQueueCapacity(500));
    }

    // ----- Explicit value wins over a Configuration value, even when smaller -----

    @Test
    void natsReader_explicitWinsOverConfiguration() throws Exception {
        Configuration conf = new Configuration();
        conf.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 100);

        assertEquals(FLINK_DEFAULT, natsReaderQueueCapacity(FLINK_DEFAULT, conf));
        assertEquals(25,  natsReaderQueueCapacity(25,  conf));
        assertEquals(100, natsReaderQueueCapacity(100, conf));
        assertEquals(200, natsReaderQueueCapacity(200, conf));   // explicit above also wins
    }

    @Test
    void jetStreamReader_explicitWinsOverConfiguration() throws Exception {
        Configuration conf = new Configuration();
        conf.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 100);

        assertEquals(FLINK_DEFAULT, jetStreamReaderQueueCapacity(FLINK_DEFAULT, conf));
        assertEquals(25,  jetStreamReaderQueueCapacity(25,  conf));
        assertEquals(100, jetStreamReaderQueueCapacity(100, conf));
        assertEquals(200, jetStreamReaderQueueCapacity(200, conf));
    }

    // ----- The -1 sentinel falls back to compile-time default when no config -----

    @Test
    void natsReader_unsetFallsBackToCompileTimeDefault() throws Exception {
        assertEquals(FLINK_DEFAULT, natsReaderQueueCapacity(-1));
    }

    @Test
    void jetStreamReader_unsetFallsBackToCompileTimeDefault() throws Exception {
        assertEquals(FLINK_DEFAULT, jetStreamReaderQueueCapacity(-1));
    }

    // ----- The -1 sentinel falls back to Configuration value when set -----

    @Test
    void natsReader_unsetFallsBackToConfiguration() throws Exception {
        Configuration conf = new Configuration();
        conf.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 50);

        assertEquals(50, natsReaderQueueCapacity(-1, conf));
    }

    @Test
    void jetStreamReader_unsetFallsBackToConfiguration() throws Exception {
        Configuration conf = new Configuration();
        conf.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 50);

        assertEquals(50, jetStreamReaderQueueCapacity(-1, conf));
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
            return reader.getQueueCapacity();
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
            return reader.getQueueCapacity();
        }
    }

    private static SourceReaderContext contextWith(Configuration conf) {
        SourceReaderContext ctx = mock(SourceReaderContext.class);
        when(ctx.getConfiguration()).thenReturn(conf);
        return ctx;
    }
}
