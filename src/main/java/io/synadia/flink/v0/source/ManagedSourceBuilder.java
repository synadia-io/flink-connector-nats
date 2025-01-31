// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source;

import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.utils.Constants;
import io.synadia.flink.v0.utils.PropertiesUtils;
import io.synadia.flink.v0.utils.SinkOrSourceBuilderBase;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;

import java.util.*;

import static io.synadia.flink.v0.utils.Constants.*;

public class ManagedSourceBuilder<OutputT> extends SinkOrSourceBuilderBase<ManagedSourceBuilder<OutputT>> {
    private PayloadDeserializer<OutputT> payloadDeserializer;
    private String payloadDeserializerClass;
    private int messageQueueCapacity = DEFAULT_ELEMENT_QUEUE_CAPACITY;
    private ManagedConsumeOptions defaultManagedConsumeOptions;
    private Configuration configuration;
    private final Map<String, ManagedSubjectConfiguration> configById = new HashMap<>();

    public ManagedSourceBuilder() {
        super(SOURCE_PREFIX);
    }

    @Override
    protected ManagedSourceBuilder<OutputT> getThis() {
        return this;
    }

    /**
     * Set source properties from a properties object
     * See the readme and {@link Constants} for property keys
     * @param properties the properties object
     * @return the builder
     */
    public ManagedSourceBuilder<OutputT> sourceProperties(Properties properties) {
        baseProperties(properties);

        String s = PropertiesUtils.getStringProperty(properties, PAYLOAD_DESERIALIZER, prefixes);
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
    public ManagedSourceBuilder<OutputT> payloadDeserializer(PayloadDeserializer<OutputT> payloadDeserializer) {
        this.payloadDeserializer = payloadDeserializer;
        this.payloadDeserializerClass = null;
        return this;
    }

    /**
     * Set the fully qualified name of the desired class payload deserializer for the source.
     * @param payloadDeserializerClass the serializer class name.
     * @return the builder
     */
    public ManagedSourceBuilder<OutputT> payloadDeserializerClass(String payloadDeserializerClass) {
        this.payloadDeserializer = null;
        this.payloadDeserializerClass = payloadDeserializerClass;
        return this;
    }

    public ManagedSourceBuilder<OutputT> messageQueueCapacity(int messageQueueCapacity) {
        this.messageQueueCapacity = messageQueueCapacity < 1 ? DEFAULT_ELEMENT_QUEUE_CAPACITY : messageQueueCapacity;
        return this;
    }

    public ManagedSourceBuilder<OutputT> defaultManagedConsumeOptions(ManagedConsumeOptions defaultManagedConsumeOptions) {
        this.defaultManagedConsumeOptions = defaultManagedConsumeOptions;
        return this;
    }

    public ManagedSourceBuilder<OutputT> configuration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public ManagedSourceBuilder<OutputT> subjectConfigurations(List<ManagedSubjectConfiguration> subjectConfigurations) {
        configById.clear();
        if (subjectConfigurations != null && !subjectConfigurations.isEmpty()) {
            for (ManagedSubjectConfiguration subjectConfiguration : subjectConfigurations) {
                configById.put(subjectConfiguration.configId, subjectConfiguration);
            }
        }
        return this;
    }

    public ManagedSourceBuilder<OutputT> subjectConfigurations(ManagedSubjectConfiguration... subjectConfigurations) {
        configById.clear();
        if (subjectConfigurations != null) {
            for (ManagedSubjectConfiguration subjectConfiguration : subjectConfigurations) {
                configById.put(subjectConfiguration.configId, subjectConfiguration);
            }
        }
        return this;
    }

    public ManagedSourceBuilder<OutputT> addSubjectConfiguration(ManagedSubjectConfiguration subjectConfiguration) {
        if (subjectConfiguration != null) {
            configById.put(subjectConfiguration.configId, subjectConfiguration);
        }
        return this;
    }

    public ManagedSource<OutputT> build() {
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
        Boundedness bTarget = defaultManagedConsumeOptions == null ? null : defaultManagedConsumeOptions.getBoundedness();
        for (ManagedSubjectConfiguration msc : configById.values()) {
            Boundedness bMsc = msc.getManagedConsumeOptions() == null ? null : msc.getManagedConsumeOptions().getBoundedness();
            if (bMsc != null) {
                if (bTarget == null) {
                    bTarget = bMsc;
                }
                else if (bMsc != bTarget) {
                    throw new IllegalStateException("All configuration Boundedness must be the same.");
                }
            }
        }

        if (defaultManagedConsumeOptions == null) {
            defaultManagedConsumeOptions = new ManagedConsumeOptions(bTarget == null ? Boundedness.CONTINUOUS_UNBOUNDED : bTarget);
        }

        // go back through the subject configs and add a managed consume option for ones that don't have one
        // make a list and then update the map
        List<ManagedSubjectConfiguration> replacements = new ArrayList<>();
        for (ManagedSubjectConfiguration orig : configById.values()) {
            if (orig.getManagedConsumeOptions() == null) {
                replacements.add(new ManagedSubjectConfiguration(orig, defaultManagedConsumeOptions));
            }
        }

        for (ManagedSubjectConfiguration replacement : replacements) {
            configById.put(replacement.configId, replacement);
        }

        if (configuration == null) {
            configuration = new Configuration();
        }
        if (!configuration.contains(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY)) {
            configuration.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, messageQueueCapacity);
        }

        return new ManagedSource<>(
            payloadDeserializer,
            defaultManagedConsumeOptions.getBoundedness(),
            configById,
            createConnectionFactory(),
            configuration);
    }
}
