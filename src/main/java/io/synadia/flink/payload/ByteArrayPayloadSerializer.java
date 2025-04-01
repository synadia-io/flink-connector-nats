// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.payload;

/**
 * A ByteArrayPayloadSerializer takes a Byte[] and converts it to a byte array.
 */
public class ByteArrayPayloadSerializer implements PayloadSerializer<Byte[]> {
    private static final long serialVersionUID = 1L;

    @Override
    public byte[] getBytes(Byte[] input) {
        byte[] bytes = new byte[input.length];
        for(int i = 0; i < input.length; i++){
            bytes[i] = input[i];
        }
        return bytes;
    }
}
