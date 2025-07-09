// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.examples.do_not_ack;

import io.nats.client.Message;
import io.nats.client.support.JsonUtils;
import io.synadia.flink.message.SourceConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;

public class JetStreamDoNotAckSourceConverter implements SourceConverter<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String convert(Message message) {
        StringBuilder stringBuilder = JsonUtils.beginJson();
        String data = new String(message.getData(), StandardCharsets.UTF_8);
        JsonUtils.addField(stringBuilder, "data", data);
        JsonUtils.addField(stringBuilder, "reply_to", message.getReplyTo());
        return JsonUtils.endJson(stringBuilder).toString();
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}

