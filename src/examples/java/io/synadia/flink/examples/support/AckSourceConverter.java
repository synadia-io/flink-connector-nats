// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.examples.support;

import io.nats.client.Message;
import io.synadia.flink.message.SourceConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;

public class AckSourceConverter implements SourceConverter<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String convert(Message message) {
        String data = new String(message.getData(), StandardCharsets.UTF_8);
        return data + "|" + message.getReplyTo() + "|" + message.metaData().streamSequence();
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
