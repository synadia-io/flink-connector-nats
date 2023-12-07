// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.sink.js;

import io.synadia.flink.Utils;
import io.synadia.flink.common.NatsSinkOrSourceBuilder;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.util.List;
import java.util.Properties;

import static io.synadia.flink.Constants.*;

public class NATSJetstreamSinkBuilder<InputT> extends NatsSinkOrSourceBuilder<NATSJetstreamSinkBuilder<InputT>> {

    private SerializationSchema<InputT> serializationSchema;
    private  NATSStreamConfig natsStreamConfig;
    @Override
    protected NATSJetstreamSinkBuilder<InputT> getThis() {return this;}

    public NATSJetstreamSinkBuilder<InputT> payloadSerializer(SerializationSchema<InputT> serializationSchema) {
    this.serializationSchema= serializationSchema;
        return this;
    }

    /**
     * Set sink properties from a properties object
     * See the readme and {@link io.synadia.flink.Constants} for property keys
     * @param properties the properties object
     * @return the builder
     */
    public NATSJetstreamSinkBuilder<InputT> sinkProperties(Properties properties) {
        List<String> subjects = Utils.getPropertyAsList(properties, SINK_SUBJECTS);
        if (!subjects.isEmpty()) {
            subjects(subjects);
        }


        long l = Utils.getLongProperty(properties, SINK_STARTUP_JITTER_MIN, -1);
        if (l != -1) {
            minConnectionJitter(l);
        }

        l = Utils.getLongProperty(properties, SINK_STARTUP_JITTER_MAX, -1);
        if (l != -1) {
            maxConnectionJitter(l);
        }

        return this;
    }

    public NATSJetstreamSinkBuilder<InputT> streamConfig(NATSStreamConfig config){
        this.natsStreamConfig = config;
        return this;
    }

    /**
     * Build a NatsSink. Subject and
     * @return the sink
     */

    public NATSJetstreamSink<InputT> build(){
        beforeBuild();
        if(serializationSchema == null){
            throw new IllegalStateException("Valid payload serializer class must be provided.");
        }
        return new NATSJetstreamSink<>(serializationSchema, createConnectionFactory(), subjects.get(0),natsStreamConfig);

    }
}

