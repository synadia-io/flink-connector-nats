// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

/**
 * An AbstractStringSinkConverter takes a String from a
 * source and creates a SinkMessage by converting
 * the string to a byte array using the provided Charset
 */
public abstract class AbstractStringSinkConverter implements SinkConverter<String> {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * The character set
     */
    private transient Charset charset;

    /**
     * The name of the character set
     */
    private final String charsetName;

    /**
     * Construct an AbstractStringSinkConverter with the supplied charset
     * @param charset the charset
     */
    public AbstractStringSinkConverter(Charset charset) {
        this.charset = charset;
        charsetName = charset.name();
    }

    /**
     * Construct an AbstractStringSinkConverter with the provided character set.
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
    public AbstractStringSinkConverter(String charsetName) {
        this(Charset.forName(charsetName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SinkMessage convert(String input) {
        return new SinkMessage(input.getBytes(charset));
    }

    /**
     * {@inheritDoc}
     */
    @Serial
    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        charset = Charset.forName(charsetName);
    }
}
