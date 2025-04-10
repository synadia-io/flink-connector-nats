// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.payload;

import io.nats.client.Message;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;

/**
 * A StringPayloadDeserializer uses the message data byte array and converts it to a String.
 */
public class StringPayloadDeserializer implements PayloadDeserializer<String> {
    private static final long serialVersionUID = 1L;

    private transient Charset charset;
    private final String charsetName;

    /**
     * Construct a StringPayloadDeserializer with the default character set, UTF-8
     */
    public StringPayloadDeserializer() {
        this(StandardCharsets.UTF_8);
    }

    /**
     * Construct a StringPayloadDeserializer with the supplied charset
     * @param charset the charset
     */
    public StringPayloadDeserializer(Charset charset) {
        this.charset = charset;
        charsetName = charset.name();
    }

    /**
     * Construct a StringPayloadDeserializer with the provided character set.
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
    public StringPayloadDeserializer(String charsetName) {
        this(Charset.forName(charsetName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getObject(Message message) {
        byte[] input = message.getData();
        if (input == null || input.length == 0) {
            return "";
        }
        return new String(input, charset);
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        charset = Charset.forName(charsetName);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
