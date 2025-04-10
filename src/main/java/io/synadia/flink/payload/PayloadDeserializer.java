// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.payload;

import io.nats.client.Message;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

public interface PayloadDeserializer<OutputT> extends Serializable, ResultTypeQueryable<OutputT> {
    /**
     * Convert a Message into an instance of the output type.
     * @return the output object
     */
    OutputT getObject(Message message);
}
