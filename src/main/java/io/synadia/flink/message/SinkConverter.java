// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.message;

import java.io.Serializable;

/**
 * A sink receives an input payload from a source.
 * An implementation of this interface will read that input
 * and build a SinkMessage containing message payload and
 * optional headers to be published.
 * @param <InputT> The expected type received from a sink.
 */
public interface SinkConverter<InputT> extends Serializable {
    /**
     * Create a SinkMessage based on the input object given to the sink.
     * If you return null, no messages will be published for this input.
     * @param input the input object
     * @return The SinkMessage object or null.
     */
    SinkMessage convert(InputT input);
}
