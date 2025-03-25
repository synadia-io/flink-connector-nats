// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.payload;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

/**
 * A StringPayloadDeserializer uses the message data byte array and converts it to a String.
 */
public class StringPayloadDeserializer implements PayloadDeserializer<String> {
    private static final long serialVersionUID = 1L;

    private final String charsetName;

    private transient Charset charset;

    /**
     * Construct a StringPayloadDeserializer for the default character set, UTF-8
     */
    public StringPayloadDeserializer() {
        this("UTF-8");
    }

    /**
     * Construct a StringPayloadDeserializer for the provided character set.
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
        Charset tempInCaseException = Charset.forName(charsetName);
        this.charsetName = charsetName;
        this.charset = tempInCaseException;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getObject(MessageRecord record) {
        byte[] input = record.message.getData();
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
