package io.synadia.io.synadia.flink;

import io.synadia.flink.payload.PayloadSerializer;

public class WordCountSerializer implements PayloadSerializer<WordCount> {
    @Override
    public byte[] getBytes(WordCount input) {
        return input.serialize();
    }
}
