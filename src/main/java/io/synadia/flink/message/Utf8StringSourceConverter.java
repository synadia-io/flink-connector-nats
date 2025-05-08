// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.message;

import java.nio.charset.StandardCharsets;

/**
 * A Utf8StringSourceConverter uses the message's data byte array
 * assuming that it is a UTF-8 String, and copies it to a Byte[]
 * object for output to a sink.
 */
public class Utf8StringSourceConverter extends AbstractStringSourceConverter {

    /**
     * Construct a Utf8StringSourceConverter
     */
    public Utf8StringSourceConverter() {
        super(StandardCharsets.UTF_8);
    }
}
