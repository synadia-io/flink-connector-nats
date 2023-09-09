// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WordSubscriber implements MessageHandler {
    public final Dispatcher d;
    final Map<String, Integer> resultMap = new HashMap<>();

    public WordSubscriber(Connection nc, String subject) {
        d = nc.createDispatcher();
        d.subscribe(subject, this);
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
