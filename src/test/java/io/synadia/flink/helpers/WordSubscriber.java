// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.helpers;

import io.nats.client.*;
import io.synadia.flink.TestBase;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WordSubscriber implements MessageHandler {
    public final Dispatcher d;
    public final JetStream js;
    final Map<String, Integer> resultMap = new HashMap<>();

    public WordSubscriber(Connection nc, String subject) throws IOException, JetStreamApiException {
        this(nc, subject, false);
    }

    public WordSubscriber(Connection nc, String subject, boolean jetStream) throws IOException, JetStreamApiException {
        d = nc.createDispatcher();
        if (jetStream) {
            js = nc.jetStream();
            js.subscribe(subject, d, this, true);
        }
        else {
            js = null;
            d.subscribe(subject, this);
        }
    }

    @Override
    public void onMessage(Message message) throws InterruptedException {
        String payload = new String(message.getData());
        resultMap.merge(payload.toLowerCase(), 1, Integer::sum);
    }

    public void assertAllMessagesReceived() {
        Assertions.assertEquals(TestBase.WORD_COUNT_MAP.size(), resultMap.size());
        for (String key : resultMap.keySet()) {
            Integer ri = resultMap.get(key);
            Integer tri = TestBase.WORD_COUNT_MAP.get(key);
            assertEquals(ri, tri);
        }
    }
}
