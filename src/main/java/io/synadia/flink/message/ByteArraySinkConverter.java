// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.message;

/**
 * A ByteArraySinkConverter takes a byte array from a
 * source and converts it to a SinkMessage
 */
public class ByteArraySinkConverter implements SinkConverter<Byte[]> {
    private static final long serialVersionUID = 1L;

    /**
     * {@inheritDoc}
     */
    @Override
    public SinkMessage convert(Byte[] input) {
        byte[] payload = new byte[input.length];
        for(int i = 0; i < input.length; i++){
            payload[i] = input[i];
        }
        return new SinkMessage(payload);
    }
}
