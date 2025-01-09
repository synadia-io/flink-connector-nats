// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.payload;

import io.nats.client.impl.Headers;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

public interface PayloadDeserializer<OutputT> extends Serializable, ResultTypeQueryable<OutputT> {

    /**
     * Get an object from message payload bytes
     *
     * @param subject the message subject
     * @param input   the input bytes.
     * @param headers the message headers
     * @return the output object
     */
    OutputT getObject(String subject, byte[] input, Headers headers);
}
