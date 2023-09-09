// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import org.apache.flink.api.connector.sink2.SinkWriter.Context;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;
import java.util.Properties;

import static io.synadia.Constants.SERIALIZER_CHARSET;

public class StringPayloadDeserializer implements PayloadDeserializer<String> {
    private static final long serialVersionUID = 1L;

    private String charsetName;

    private transient Charset charset;

    public StringPayloadDeserializer() {
        setCharsetName("UTF-8");
    }

    public StringPayloadDeserializer(String charsetName) {
        setCharsetName(charsetName);
    }

    @Override
    public void init(Properties serializerProperties) {
        charsetName = serializerProperties.getProperty(SERIALIZER_CHARSET);
        if (charsetName != null) {
            setCharsetName(charsetName);
        }
    }

    @Override
    public String getObject(byte[] input, Context context) {
        return new String(input, charset);
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
        prepareCharset();
    }

    private void prepareCharset() {
        charset = Charset.forName(charsetName);
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        prepareCharset();
    }
}
