// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink.message;

import io.nats.client.impl.Headers;
import io.synadia.flink.TestBase;
import io.synadia.flink.message.SinkMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class SinkMessageTests extends TestBase {

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
}
