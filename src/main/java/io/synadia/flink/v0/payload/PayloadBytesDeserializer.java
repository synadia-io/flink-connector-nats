package io.synadia.flink.v0.payload;

import io.nats.client.impl.Headers;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A PayloadByteArrayDeserializer takes payload details and returns a Payload object.
 */
public class PayloadBytesDeserializer implements PayloadDeserializer<Payload<byte[]>> {

    @Override
    public Payload<byte[]> getObject(String subject, byte[] data, Headers headers, String replyTo) {
        Map<String, List<String>> newHeaders = new HashMap<>();

        // collect required information from the headers
        if (headers != null) {
            headers.forEach(newHeaders::put);
        }

        return new Payload<>(data, newHeaders, replyTo);
    }

    @Override
    public TypeInformation<Payload<byte[]>> getProducedType() {
        return TypeInformation.of(new TypeHint<Payload<byte[]>>() {});
    }
}