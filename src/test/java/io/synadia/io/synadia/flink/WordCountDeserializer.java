package io.synadia.io.synadia.flink;

import io.synadia.flink.Utils;
import io.synadia.flink.payload.PayloadDeserializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class WordCountDeserializer implements PayloadDeserializer<WordCount> {
    @Override
    public WordCount getObject(byte[] input) {
        return new WordCount(input);
    }

    @Override
    public TypeInformation<WordCount> getProducedType() {
        return Utils.getTypeInformation(WordCount.class);
    }
}
