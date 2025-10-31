// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.message;

import java.nio.charset.StandardCharsets;

/**
 * An AsciiStringSinkConverter takes a String from a
 * source and converts it to a SinkMessage assuming
 * the string is an ASCII string
 */
public class AsciiStringSinkConverter extends AbstractStringSinkConverter {
    /**
     * Construct an AsciiStringSinkConverter
     */
    public AsciiStringSinkConverter() {
        super(StandardCharsets.US_ASCII);
    }
}
