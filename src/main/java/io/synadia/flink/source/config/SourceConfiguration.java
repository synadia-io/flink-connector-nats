package io.synadia.flink.source.config;

import io.nats.client.support.SerializableConsumerConfiguration;
import java.time.Duration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;

public class SourceConfiguration extends Configuration {

    private static final long serialVersionUID = 8488507275800787580L;

    private  int messageQueueCapacity;
    private  boolean enableAutoAcknowledgeMessage;
    private  Duration fetchOneMessageTime;
    private  Duration maxFetchTime;
    private  int maxFetchRecords;
    private  String consumerName;
    private String subjectName;
    private String url;
    private Duration natsAutoAckInterval;


    /**
     * Creates a new UnmodifiableConfiguration, which holds a copy of the given configuration that
     * cannot be altered.
     *
     * @param config The configuration with the original contents.
     */
    //TODO Pick from client provided configuration
    public SourceConfiguration() {
        messageQueueCapacity = get(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY);
        maxFetchTime = get(NatsJetstreamSourceOptions.NATS_FETCH_TIMEOUT);
        maxFetchRecords = get(NatsJetstreamSourceOptions.NATS_CONSUMER_MAX_FETCH_RECORD);;
        fetchOneMessageTime = get(NatsJetstreamSourceOptions.NATS_CONSUMER_FETCH_ONE_MESSAGE_TIME);
        natsAutoAckInterval = get(NatsJetstreamSourceOptions.NATS_AUTO_ACK_INTERVAL);
        enableAutoAcknowledgeMessage = get(NatsJetstreamSourceOptions.ENABLE_AUTO_ACK);
    }

    public SourceConfiguration(String subject, String url, SerializableConsumerConfiguration consumerConfiguration) {
        this();
        this.consumerName = consumerConfiguration.getConsumerConfiguration().getDurable();
        this.url = url;
        this.subjectName = subject;

    }

    public int getMessageQueueCapacity() {
        return messageQueueCapacity;
    }

    public boolean isEnableAutoAcknowledgeMessage() {
        return enableAutoAcknowledgeMessage;
    }

    public Duration getFetchOneMessageTime() {
        return fetchOneMessageTime;
    }

    public Duration getMaxFetchTime() {
        return maxFetchTime;
    }

    public int getMaxFetchRecords() {
        return maxFetchRecords;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public String getSubjectName() {
        return subjectName;
    }

    public String getUrl() {
        return url;
    }

    public Duration getNatsAutoAckInterval() {
        return natsAutoAckInterval;
    }

}
