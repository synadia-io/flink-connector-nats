// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.message;

import java.nio.charset.StandardCharsets;

/**
 * A Utf8StringSinkConverter takes a String from a
 * source and converts it to a SinkMessage assuming
 * the string is a UTF-8 string
 */
public class Utf8StringSinkConverter extends AbstractStringSinkConverter {

    /**
     * Construct a Utf8StringSinkConverter
     */
    public Utf8StringSinkConverter() {
        super(StandardCharsets.UTF_8);
    }
}
