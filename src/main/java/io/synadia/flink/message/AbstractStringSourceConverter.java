// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.message;

import io.nats.client.Message;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

/**
 * An AbstractStringSourceConverter uses the message's data byte array
 * and copies it to a Byte[] object for output to a sink.
 * It must be subclasses to provide the Charset to use
 */
public abstract class AbstractStringSourceConverter implements SourceConverter<String> {
    private static final long serialVersionUID = 1L;

    private transient Charset charset;
    private final String charsetName;

    /**
     * Construct an AbstractStringSourceConverter with the supplied charset
     * @param charset the charset
     */
    protected AbstractStringSourceConverter(Charset charset) {
        this.charset = charset;
        charsetName = charset.name();
    }

    /**
     * Construct an AbstractStringSourceConverter with the provided character set.
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
    protected AbstractStringSourceConverter(String charsetName) {
        this(Charset.forName(charsetName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String convert(Message message) {
        byte[] input = message.getData();
        if (input.length == 0) {
            return "";
        }
        return new String(input, charset);
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        charset = Charset.forName(charsetName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
