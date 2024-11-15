// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.payload;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

/**
 * A StringPayloadSerializer takes a String and converts it to a byte array.
 */
public class StringPayloadSerializer implements PayloadSerializer<String> {
    private static final long serialVersionUID = 1L;

    private final String charsetName;

    private transient Charset charset;

    /**
     * Construct a StringPayloadSerializer for the default character set, UTF-8
     */
    public StringPayloadSerializer() {
        this("UTF-8");
    }

    /**
     * Construct a StringPayloadSerializer for the provided character set.
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
        Charset tempInCaseException = Charset.forName(charsetName);
        this.charsetName = charsetName;
        this.charset = tempInCaseException;
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
