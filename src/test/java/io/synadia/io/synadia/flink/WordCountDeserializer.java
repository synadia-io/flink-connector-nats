package io.synadia.io.synadia.flink;

import io.nats.client.impl.Headers;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.utils.PropertiesUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class WordCountDeserializer implements PayloadDeserializer<WordCount> {
    @Override
    public WordCount getObject(String subject, byte[] input, Headers headers) {
        return new WordCount(input);
    }

    @Override
    public TypeInformation<WordCount> getProducedType() {
        return PropertiesUtils.getTypeInformation(WordCount.class);
    }
}
