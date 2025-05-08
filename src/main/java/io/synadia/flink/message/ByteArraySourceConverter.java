// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.message;

import io.nats.client.Message;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import static org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO;

/**
 * A ByteArraySourceConverter uses the message data byte array
 * and copies it to a Byte[] object for output to a sink.
 */
public class ByteArraySourceConverter implements SourceConverter<Byte[]> {
    private static final long serialVersionUID = 1L;

    /**
     * {@inheritDoc}
     */
    @Override
    public Byte[] convert(Message message) {
        byte[] data = message.getData();
        int len = data == null ? 0 : data.length;
        Byte[] object = new Byte[len];
        for (int x = 0; x < len; x++) {
            object[x] = data[x];
        }
        return object;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypeInformation<Byte[]> getProducedType() {
        return BYTE_ARRAY_TYPE_INFO;
    }
}
