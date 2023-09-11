// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.payload;

import org.apache.flink.api.connector.sink2.SinkWriter.Context;

import java.io.Serializable;

public interface PayloadSerializer<InputT> extends Serializable {

    /**
     * Get bytes from the input object so they can be published in a message
     * @param input the input object
     * @param context See {@link org.apache.flink.api.connector.sink.SinkWriter.Context SinkWriter.Context}
     * @return the bytes
     */
    byte[] getBytes(InputT input, Context context);
}
