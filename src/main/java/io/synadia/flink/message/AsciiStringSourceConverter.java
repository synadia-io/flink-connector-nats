// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.message;

import java.nio.charset.StandardCharsets;

/**
 * An AsciiStringSourceConverter uses the message's data byte array
 * assuming that it is an ASCII String, and copies it to a Byte[]
 * object for output to a sink.
 */
public class AsciiStringSourceConverter extends AbstractStringSourceConverter {

    /**
     * Construct an AsciiStringSourceConverter
     */
    public AsciiStringSourceConverter() {
        super(StandardCharsets.US_ASCII);
    }
}

