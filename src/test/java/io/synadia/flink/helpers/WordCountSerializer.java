// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.helpers;

import io.synadia.flink.payload.PayloadSerializer;

public class WordCountSerializer implements PayloadSerializer<WordCount> {
    @Override
    public byte[] getBytes(WordCount input) {
        return input.serialize();
    }
}
