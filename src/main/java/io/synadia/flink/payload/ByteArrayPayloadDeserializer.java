// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.payload;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import static org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO;

/**
 * A ByteArrayPayloadDeserializer uses the message data byte array and copys it to a Byte[] object.
 */
public class ByteArrayPayloadDeserializer implements PayloadDeserializer<Byte[]> {
    private static final long serialVersionUID = 1L;

    @Override
    public Byte[] getObject(MessageRecord record) {
        byte[] data = record.message.getData();
        int len = data == null ? 0 : data.length;
        Byte[] object = new Byte[len];
        for (int x = 0; x < len; x++) {
            object[x] = data[x];
        }
        return object;
    }

    @Override
    public TypeInformation<Byte[]> getProducedType() {
        return BYTE_ARRAY_TYPE_INFO;
    }
}
