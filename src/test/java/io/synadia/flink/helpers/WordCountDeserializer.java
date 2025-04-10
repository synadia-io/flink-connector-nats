// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.helpers;

import io.nats.client.Message;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.utils.MiscUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class WordCountDeserializer implements PayloadDeserializer<WordCount> {
    @Override
    public WordCount getObject(Message message) {
        return new WordCount(message.getData());
    }

    @Override
    public TypeInformation<WordCount> getProducedType() {
        return MiscUtils.getTypeInformation(WordCount.class);
    }
}
