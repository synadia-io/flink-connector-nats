package io.synadia.flink.source.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

@PublicEvolving
@ConfigGroups(
        groups = {
                @ConfigGroup(name="NatsJetStreamSource", keyPrefix = "nats.source")
        }

)
public final class NatsJetstreamSourceOptions {


    private static final String SOURCE_CONFIG_PREFIX = "nats.source";

    private NatsJetstreamSourceOptions(){

    }

    public static final ConfigOption<Duration> NATS_CONSUMER_FETCH_ONE_MESSAGE_TIME =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "fetchOneTimeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(5000));

    public static final ConfigOption<Integer> NATS_CONSUMER_MAX_FETCH_RECORD =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "maxFetchRecords")
                    .intType().defaultValue(100);

    public static final ConfigOption<Duration> NATS_FETCH_TIMEOUT = ConfigOptions.key(SOURCE_CONFIG_PREFIX + "fetchTimeout")
            .durationType()
            .defaultValue(Duration.ofMillis(5000));;

    public static final ConfigOption<Duration> NATS_AUTO_ACK_INTERVAL = ConfigOptions.key(SOURCE_CONFIG_PREFIX + "autoAckInterval")
            .durationType().defaultValue(Duration.ofMillis(5000));

    public static final ConfigOption<Boolean> ENABLE_AUTO_ACK = ConfigOptions.key(SOURCE_CONFIG_PREFIX + "enableAutoAck")
            .booleanType()
            .defaultValue(false);

}
