// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.message;

import io.nats.client.Message;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

/**
 * A source receives a message from the NATS server.
 * An implementation of this interface will read that message
 * and generate the output for sinks.
 * @param <OutputT> The type that the source is expected to output.
 */
public interface SourceConverter<OutputT> extends Serializable, ResultTypeQueryable<OutputT> {
    /**
     * Read a message and to create an instance of the output type.
     * @return the output object
     */
    OutputT convert(Message message);
}
