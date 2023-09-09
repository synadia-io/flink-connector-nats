// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import org.apache.flink.api.connector.sink2.SinkWriter.Context;

import java.io.Serializable;
import java.util.Properties;

public interface PayloadDeserializer<OutputT> extends Serializable {

    default void init(Properties serializerProperties) {}

    OutputT getObject(byte[] input, Context context);
}
