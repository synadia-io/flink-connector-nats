// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.utils.Constants;
import io.synadia.flink.utils.PropertiesUtils;
import io.synadia.flink.utils.SinkOrSourceBuilderBase;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JetStreamSourceBuilder<OutputT> extends SinkOrSourceBuilderBase<JetStreamSourceBuilder<OutputT>> {
    private PayloadDeserializer<OutputT> payloadDeserializer;
    private String payloadDeserializerClass;
    private int messageQueueCapacity = Constants.DEFAULT_ELEMENT_QUEUE_CAPACITY;
    private Configuration configuration;
    private final Map<String, JetStreamSubjectConfiguration> configById = new HashMap<>();

    public JetStreamSourceBuilder() {
        super(Constants.SOURCE_PREFIX);
    }

    @Override
    protected JetStreamSourceBuilder<OutputT> getThis() {
        return this;
    }

    /**
     * Set source properties from a properties object
     * See the readme and {@link Constants} for property keys
     * @param properties the properties object
     * @return the builder
     */
    public JetStreamSourceBuilder<OutputT> sourceProperties(Properties properties) {
        baseProperties(properties);

        String s = PropertiesUtils.getStringProperty(properties, Constants.PAYLOAD_DESERIALIZER, prefixes);
        if (s != null) {
            payloadDeserializerClass(s);
        }
        return this;
    }

    /**
     * Set the payload deserializer for the source.
     * @param payloadDeserializer the deserializer.
     * @return the builder
     */
    public JetStreamSourceBuilder<OutputT> payloadDeserializer(PayloadDeserializer<OutputT> payloadDeserializer) {
        this.payloadDeserializer = payloadDeserializer;
        this.payloadDeserializerClass = null;
        return this;
    }

    /**
     * Set the fully qualified name of the desired class payload deserializer for the source.
     * @param payloadDeserializerClass the serializer class name.
     * @return the builder
     */
    public JetStreamSourceBuilder<OutputT> payloadDeserializerClass(String payloadDeserializerClass) {
        this.payloadDeserializer = null;
        this.payloadDeserializerClass = payloadDeserializerClass;
        return this;
    }

    public JetStreamSourceBuilder<OutputT> messageQueueCapacity(int messageQueueCapacity) {
        this.messageQueueCapacity = messageQueueCapacity < 1 ? Constants.DEFAULT_ELEMENT_QUEUE_CAPACITY : messageQueueCapacity;
        return this;
    }

    public JetStreamSourceBuilder<OutputT> configuration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public JetStreamSourceBuilder<OutputT> setSubjectConfigurations(JetStreamSubjectConfiguration... subjectConfigurations) {
        configById.clear();
        return addSubjectConfigurations(subjectConfigurations);
    }

    public JetStreamSourceBuilder<OutputT> setSubjectConfigurations(List<JetStreamSubjectConfiguration> subjectConfigurations) {
        configById.clear();
        return addSubjectConfigurations(subjectConfigurations);
    }

    public JetStreamSourceBuilder<OutputT> addSubjectConfigurations(JetStreamSubjectConfiguration... subjectConfigurations) {
        if (subjectConfigurations != null) {
            for (JetStreamSubjectConfiguration subjectConfiguration : subjectConfigurations) {
                if (subjectConfiguration != null) {
                    configById.put(subjectConfiguration.configId, subjectConfiguration);
                }
            }
        }
        return this;
    }

    public JetStreamSourceBuilder<OutputT> addSubjectConfigurations(List<JetStreamSubjectConfiguration> subjectConfigurations) {
        if (subjectConfigurations != null) {
            for (JetStreamSubjectConfiguration subjectConfiguration : subjectConfigurations) {
                if (subjectConfiguration != null) {
                    configById.put(subjectConfiguration.configId, subjectConfiguration);
                }
            }
        }
        return this;
    }

    public JetStreamSource<OutputT> build() {
        if (payloadDeserializer == null) {
            if (payloadDeserializerClass == null) {
                throw new IllegalStateException("Valid payload deserializer class must be provided.");
            }

            // so much can go wrong here... ClassNotFoundException, ClassCastException
            try {
                //noinspection unchecked
                payloadDeserializer = (PayloadDeserializer<OutputT>) Class.forName(payloadDeserializerClass).getDeclaredConstructor().newInstance();
            }
            catch (Exception e) {
                throw new IllegalStateException("Valid payload serializer class must be provided.", e);
            }
        }
        baseBuild(false);

        if (configById.isEmpty()) {
            throw new IllegalStateException("At least 1 managed subject configuration is required");
        }

        // check all the consume options of all the subject configs to
        // make sure they are the same boundedness if they are supplied
        Boundedness boundedness = null;
        for (JetStreamSubjectConfiguration msc : configById.values()) {
            if (boundedness == null) {
                boundedness = msc.boundedness;
            }
            else if (boundedness != msc.boundedness) {
                throw new IllegalStateException("All boundedness must be the same.");
            }
        }

        if (configuration == null) {
            configuration = new Configuration();
        }
        if (!configuration.contains(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY)) {
            configuration.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, messageQueueCapacity);
        }

        return new JetStreamSource<>(
            payloadDeserializer,
            boundedness,
            configById,
            createConnectionFactory(),
            configuration);
    }
}
