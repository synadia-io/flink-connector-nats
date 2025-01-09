// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.io.synadia.flink;

import io.nats.client.support.*;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Objects;

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

public class WordCount implements JsonSerializable {
    public String word;
    public int count;

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
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, "word", word);
        JsonUtils.addField(sb, "count", count);
        return endJson(sb).toString();
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
