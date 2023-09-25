package io.synadia.flink.payload;

import io.nats.client.impl.Headers;

public class Payload<InputT> {
    public final InputT payload;
    public final Headers headers;

    public Payload(InputT payload) {
        this.payload = payload;
        this.headers = null;
    }

    public Payload(InputT payload, Headers headers) {
        this.payload = payload;
        this.headers = headers;
    }
}
