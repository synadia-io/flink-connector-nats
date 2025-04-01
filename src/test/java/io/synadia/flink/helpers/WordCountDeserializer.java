// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.helpers;

import io.synadia.flink.payload.MessageRecord;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.utils.PropertiesUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class WordCountDeserializer implements PayloadDeserializer<WordCount> {
    @Override
    public WordCount getObject(MessageRecord record) {
        return new WordCount(record.message.getData());
    }

    @Override
    public TypeInformation<WordCount> getProducedType() {
        return PropertiesUtils.getTypeInformation(WordCount.class);
    }
}
