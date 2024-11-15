// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.payload;

import java.io.Serializable;

public interface PayloadSerializer<InputT> extends Serializable {

    /**
     * Get bytes from the input object so they can be published in a message
     *
     * @param input the input object
     * @return the bytes
     */
    byte[] getBytes(InputT input);
}
