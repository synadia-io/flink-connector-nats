// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.payload;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;

/**
 * A StringPayloadSerializer takes a String and converts it to a byte array.
 */
public class StringPayloadSerializer implements PayloadSerializer<String> {
    private static final long serialVersionUID = 1L;

    private final String charsetName;

    private transient Charset charset;

    /**
     * Construct a StringPayloadSerializer with the default character set, UTF-8
     */
    public StringPayloadSerializer() {
        this(StandardCharsets.UTF_8);
    }

    /**
     * Construct a StringPayloadSerializer with the supplied charset
     * @param charset the charset
     */
    public StringPayloadSerializer(Charset charset) {
        this.charset = charset;
        charsetName = charset.name();
    }

    /**
     * Construct a StringPayloadSerializer with the provided character set.
     * @param  charsetName
     *         The name of the requested charset; may be either
     *         a canonical name or an alias
     * @throws IllegalCharsetNameException
     *          If the given charset name is illegal
     * @throws  IllegalArgumentException
     *          If the given {@code charsetName} is null
     * @throws UnsupportedCharsetException
     *          If no support for the named charset is available
     *          in this instance of the Java virtual machine
     */
    public StringPayloadSerializer(String charsetName) {
        this(Charset.forName(charsetName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getBytes(String input) {
        return input.getBytes(charset);
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        charset = Charset.forName(charsetName);
    }
}
