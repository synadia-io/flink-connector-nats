package io.synadia.flink.v0.payload;

import java.io.*;

/**
 * A serializer for Payload objects that contain byte arrays as their payload.
 */
public class PayloadBytesSerializer implements PayloadSerializer<Payload<byte[]>> {

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getBytes(Payload<byte[]> input) {
        return (input != null && input.data != null) ? input.data : new byte[0];
    }
}