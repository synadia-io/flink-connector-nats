package io.synadia.flink.source;

import static io.synadia.flink.Constants.SOURCE_STARTUP_JITTER_MAX;
import static io.synadia.flink.Constants.SOURCE_STARTUP_JITTER_MIN;
import static io.synadia.flink.Constants.SOURCE_SUBJECTS;
import io.synadia.flink.Utils;
import io.synadia.flink.common.NatsSinkOrSourceBuilder;
import java.util.List;
import java.util.Properties;
import org.apache.flink.api.common.serialization.DeserializationSchema;

public class NATSJetstreamSourceBuilder<T> extends NatsSinkOrSourceBuilder<NATSJetstreamSourceBuilder<T>> {

    private DeserializationSchema<T> deserializationSchema;

    private NATSConsumerConfig natsConsumerConfig;

    @Override
    protected NATSJetstreamSourceBuilder<T> getThis() {
        return this;
    }

    /**
     * Set the deserializer for the source.
     * @param deserializationSchema the deserializer.
     * @return the builder
     */
    public NATSJetstreamSourceBuilder<T> payloadDeserializer(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    /**
     * Set source properties from a properties object
     * See the readme and {@link io.synadia.flink.Constants} for property keys
     * @param properties the properties object
     * @return the builder
     */
    public NATSJetstreamSourceBuilder<T> sourceProperties(Properties properties) {
        List<String> subjects = Utils.getPropertyAsList(properties, SOURCE_SUBJECTS);
        if (!subjects.isEmpty()) {
            subjects(subjects);
        }

        long l = Utils.getLongProperty(properties, SOURCE_STARTUP_JITTER_MIN, -1);
        if (l != -1) {
            minConnectionJitter(l);
        }

        l = Utils.getLongProperty(properties, SOURCE_STARTUP_JITTER_MAX, -1);
        if (l != -1) {
            maxConnectionJitter(l);
        }

        return this;
    }

    public NATSJetstreamSourceBuilder<T> consumerConfig(NATSConsumerConfig config) {
        this.natsConsumerConfig = config;
        return this;
    }

    /**
     * Build a NatsSource. Subject and
     * @return the source
     */
    public NATSJetstreamSource<T> build() {
        beforeBuild();
        if (deserializationSchema == null) {
                throw new IllegalStateException("Valid payload serializer class must be provided.");
        }
        return new NATSJetstreamSource<>(deserializationSchema, createConnectionFactory(), subjects.get(0), natsConsumerConfig);
    }
}