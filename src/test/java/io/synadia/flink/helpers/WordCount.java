// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.helpers;

import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Objects;

public class WordCount {
    public final String word;
    public final int count;

    public WordCount(byte[] json) {
        this(new String(json));
    }

    public WordCount(String json) {
        try {
            JsonValue jv = JsonParser.parse(json);
            word = JsonValueUtils.readString(jv, "word");
            count = JsonValueUtils.readInteger(jv, "count");
        }
        catch (Exception e) {
            throw new FlinkRuntimeException("Invalid Json: " + e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WordCount wordCount = (WordCount) o;

        if (count != wordCount.count) return false;
        return Objects.equals(word, wordCount.word);
    }

    @Override
    public int hashCode() {
        int result = word != null ? word.hashCode() : 0;
        result = 31 * result + count;
        return result;
    }
}
