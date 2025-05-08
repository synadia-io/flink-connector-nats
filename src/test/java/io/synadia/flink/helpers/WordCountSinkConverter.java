// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.helpers;

import io.nats.client.support.JsonUtils;
import io.synadia.flink.message.SinkConverter;
import io.synadia.flink.message.SinkMessage;

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

public class WordCountSinkConverter implements SinkConverter<WordCount> {
    @Override
    public SinkMessage convert(WordCount input) {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, "word", input.word);
        JsonUtils.addField(sb, "count", input.count);
        return new SinkMessage(endJson(sb).toString().getBytes());
    }
}
