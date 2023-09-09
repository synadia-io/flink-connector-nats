// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import io.nats.client.support.JsonParseException;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonSerializable;
import org.apache.flink.api.connector.sink2.SinkWriter.Context;
import org.apache.flink.util.FlinkRuntimeException;

public class JsonSerializablePayloadDeserializer implements PayloadDeserializer<JsonSerializable> {
    private static final long serialVersionUID = 1L;

    @Override
    public JsonSerializable getObject(byte[] input, Context context) {
        try {
            return JsonParser.parse(input);
        }
        catch (JsonParseException e) {
            throw new FlinkRuntimeException("Error deserializing message payload.", e);
        }
    }
}
