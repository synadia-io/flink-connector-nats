// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.payload;

import java.io.Serializable;

public interface PayloadDeserializer<OutputT> extends Serializable {

    /**
     * Get an object from message payload bytes
     *
     * @param input the input bytes.
     * @return the output object
     */
    OutputT getObject(byte[] input);
}
