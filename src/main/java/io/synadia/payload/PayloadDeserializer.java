// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.payload;

import org.apache.flink.api.connector.sink2.SinkWriter.Context;

import java.io.Serializable;

public interface PayloadDeserializer<OutputT> extends Serializable {

    /**
     * Get an object from message payload bytes
     * @param input the input bytes.
     * @param context See {@link org.apache.flink.api.connector.sink.SinkWriter.Context SinkWriter.Context}
     * @return the output object
     */
    OutputT getObject(byte[] input, Context context);
}
