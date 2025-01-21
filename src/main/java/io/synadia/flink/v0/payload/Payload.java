package io.synadia.flink.v0.payload;

import io.nats.client.impl.Headers;

import java.io.Serializable;
import java.util.*;

/**
 * Payload is a generic class that holds the data, headers and replyTo information.
 *
 * @param <InputT> the data type
 */
public class Payload<InputT> implements Serializable {
    public final InputT data;

    public final Map<String, List<String>> headers;
    public final String replyTo;

    public Payload() {
        this.data = null;
        this.headers = null;
        this.replyTo = null;
    }

    public Payload(InputT data, Map<String, List<String>> headers, String replyTo) {
        this.data = data;
        this.headers = headers;
        this.replyTo = replyTo;
    }

    @Override
    public String toString() {
        Set<String> keys = new HashSet<>();
        if (headers != null) {
            keys = headers.keySet();
        }

        return "Payload{" +
                "data=" + data +
                ", headers=" + keys +
                ", replyTo='" + replyTo + '\'' +
                '}';
    }
}

