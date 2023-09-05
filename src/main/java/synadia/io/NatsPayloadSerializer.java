// Copyright (c) 2023 Synadia Communications Inc.  All Rights Reserved.

package synadia.io;

import org.apache.flink.api.connector.sink2.SinkWriter.Context;

import java.io.Serializable;
import java.util.Properties;

public interface NatsPayloadSerializer<InputT> extends Serializable {

    default void init(Properties serializerProperties) {}

    byte[] getBytes(InputT input, Context context);
}
