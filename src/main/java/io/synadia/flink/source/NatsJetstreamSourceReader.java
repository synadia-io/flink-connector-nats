// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import static org.apache.flink.util.Preconditions.checkNotNull;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.Subscription;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.synadia.flink.Utils;
import io.synadia.flink.common.ConnectionFactory;
import io.synadia.flink.source.split.NatsSubjectSplit;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NatsJetstreamSourceReader<OutputT> implements SourceReader<OutputT, NatsSubjectSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(NatsJetstreamSourceReader.class);

    private final String id;
    private final ConnectionFactory connectionFactory;
    private final DeserializationSchema<OutputT> payloadDeserializer;
    private final SourceReaderContext readerContext;
    private final List<NatsSubjectSplit> subbedSplits;
    private final FutureCompletingBlockingQueue<Message> messages;
    private Connection connection;
    private Subscription subscription;
    private NatsConsumerConfig config;
    private JetStream js;
    private String subject;
    public NatsJetstreamSourceReader(String sourceId,
                            ConnectionFactory connectionFactory,
                            NatsConsumerConfig natsConsumerConfig,
                            DeserializationSchema<OutputT> payloadDeserializer,
                            SourceReaderContext readerContext,
                                     String subject) {
        id = sourceId + "-" + Utils.generatePrefixedId(sourceId);
        this.connectionFactory = connectionFactory;
        this.payloadDeserializer = payloadDeserializer;
        this.readerContext = checkNotNull(readerContext);
        subbedSplits = new ArrayList<>();
        messages = new FutureCompletingBlockingQueue<>();
        this.config= natsConsumerConfig;
        this.subject = subject;
    }


    @Override
    public void start() {
        LOG.debug("{} | start", id);
        try {
            connection = connectionFactory.connect();
            js = connection.jetStream();
            ConsumerConfiguration consumerConfiguration = ConsumerConfiguration.builder().name(config.getConsumerName())
                    .ackPolicy(AckPolicy.All).build();
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .configuration(consumerConfiguration).durable(config.getConsumerName())
                    .build();
            subscription = js.subscribe(subject, pullOptions);
        }
        catch (IOException e) {
            throw new FlinkRuntimeException(e);
        } catch (JetStreamApiException e) {
            e.printStackTrace();
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OutputT> output) throws Exception {
        List<Message> messages =
                ((JetStreamSubscription) subscription).fetch(config.getBatchSize(), Duration.ofSeconds(5));
        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            boolean ackMessageFlag = (i == messages.size() - 1);
            processMessage(output, message, ackMessageFlag);
        }
        InputStatus is = messages.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
        LOG.debug("{} | pollNext had message, then {}", id, is);
        return is;
    }

    private void processMessage(ReaderOutput<OutputT> readerOutput, Message message, boolean ackMessage) throws IOException {
        try {
            OutputT data = payloadDeserializer.deserialize(message.getData());
            readerOutput.collect(data);
            if (ackMessage) {
                message.ack();
            }
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public List<NatsSubjectSplit> snapshotState(long checkpointId) {
        LOG.debug("{} | snapshotState", id);
        return Collections.unmodifiableList(subbedSplits);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return messages.getAvailabilityFuture();
    }

    @Override
    public void addSplits(List<NatsSubjectSplit> splits) {
        for (NatsSubjectSplit split : splits) {
            LOG.debug("{} | addSplits {}", id, split);
            int ix = subbedSplits.indexOf(split);
            if (ix == -1) {
                subbedSplits.add(split);
            }
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.debug("{} | notifyNoMoreSplits", id);
    }

    @Override
    public void close() throws Exception {
        LOG.debug("{} | close", id);
        subscription.unsubscribe();
        connection.close();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        LOG.debug("{} | handleSourceEvents {}", id, sourceEvent);
    }

    @Override
    public String toString() {
        return "NatsJetstreamSourceReader{" +
                "id='" + id + '\'' +
                ", subbedSplits=" + subbedSplits +
                '}';
    }
}
