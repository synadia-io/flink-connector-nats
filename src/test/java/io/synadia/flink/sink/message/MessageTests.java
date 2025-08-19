// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink.message;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.synadia.flink.TestBase;
import io.synadia.flink.message.*;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class MessageTests extends TestBase {

    @Test
    public void testSinkMessage() {
        Headers headers = new Headers();
        byte[] bytes = "payload".getBytes(StandardCharsets.UTF_8);

        SinkMessage sm = new SinkMessage(bytes);
        assertArrayEquals(bytes, sm.payload);
        assertNull(sm.headers);

        sm = new SinkMessage(bytes, null);
        assertArrayEquals(bytes, sm.payload);
        assertNull(sm.headers);

        sm = new SinkMessage(bytes, headers);
        assertArrayEquals(bytes, sm.payload);
        assertNull(sm.headers);

        headers.put("foo", "bar");
        sm = new SinkMessage(bytes, headers);
        assertArrayEquals(bytes, sm.payload);
        assertNotNull(sm.headers);
        assertEquals("bar", sm.headers.getFirst("foo"));
    }

    @Test
    public void testByteArraySourceConverter() {
        ByteArraySourceConverter converter = new ByteArraySourceConverter();

        Message m = NatsMessage.builder()
            .subject("test")
            .data("data")
            .build();
        Byte[] bytes = converter.convert(m);
        assertNotNull(bytes);
        byte[] switched = new byte[bytes.length];
        for(int i = 0; i < bytes.length; i++){
            switched[i] = bytes[i];
        }
        assertEquals("data", new String(switched));

        m = NatsMessage.builder()
            .subject("test")
            .data("")
            .build();
        bytes = converter.convert(m);
        assertNotNull(bytes);
        assertEquals(0, bytes.length);

        m = NatsMessage.builder()
            .subject("test")
            .build();
        bytes = converter.convert(m);
        assertNotNull(bytes);
        assertEquals(0, bytes.length);
    }

    static class CoverageAbstractStringSinkConverter extends AbstractStringSinkConverter {
        public CoverageAbstractStringSinkConverter() {
            super(StandardCharsets.UTF_8.name());
        }
    }

    @Test
    public void testAbstractStringSinkConverterCoverage() {
        SinkConverter<String> utf8 = new Utf8StringSinkConverter();
        SinkConverter<String> coverage = new CoverageAbstractStringSinkConverter();
        for (String s : UTF8_TEST_STRINGS) {
            assertArrayEquals(utf8.convert(s).payload, coverage.convert(s).payload);
        }
    }

    static class CoverageAbstractStringSourceConverter extends AbstractStringSourceConverter {
        public CoverageAbstractStringSourceConverter() {
            super(StandardCharsets.UTF_8.name());
        }
    }

    @Test
    public void testAbstractStringSourceConverterCoverage() {
        SourceConverter<String> utf8 = new Utf8StringSourceConverter();
        SourceConverter<String> coverage = new CoverageAbstractStringSourceConverter();
        for (String s : UTF8_TEST_STRINGS) {
            Message m = NatsMessage.builder()
                .subject("test")
                .data(s.getBytes(StandardCharsets.UTF_8))
                .build();
            assertEquals(utf8.convert(m), coverage.convert(m));
            m = NatsMessage.builder()
                .subject("test")
                .data("")
                .build();
            assertEquals(utf8.convert(m), coverage.convert(m));
            m = NatsMessage.builder()
                .subject("test")
                .build();
            assertEquals(utf8.convert(m), coverage.convert(m));
        }
    }
}
