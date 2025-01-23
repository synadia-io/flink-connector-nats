package io.synadia.flink.v0.payload;

import io.nats.client.Message;

public class MessageRecord {
    public final Message message;

    public MessageRecord(Message message) {
        this.message = message;
    }

    public Message getMessage() {
        return message;
    }
}
