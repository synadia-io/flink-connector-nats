// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.payload;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

public interface PayloadDeserializer<OutputT> extends Serializable, ResultTypeQueryable<OutputT> {

    /**
     * Get an object from a payload object which currently just wraps a message
     * @return the output object
     */
    OutputT getObject(MessageRecord record);
}
