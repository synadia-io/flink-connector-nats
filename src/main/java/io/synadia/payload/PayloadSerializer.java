// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.payload;

import org.apache.flink.api.connector.sink2.SinkWriter.Context;

import java.io.Serializable;

public interface PayloadSerializer<InputT> extends Serializable {
    byte[] getBytes(InputT input, Context context);
}
