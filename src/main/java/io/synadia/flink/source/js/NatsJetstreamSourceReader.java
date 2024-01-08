// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.js;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.synadia.flink.Utils;
import io.synadia.flink.common.ConnectionFactory;
import io.synadia.flink.source.split.NatsSubjectSplit;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

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
    private final Boundedness mode;
    public NatsJetstreamSourceReader(String sourceId,
                                     ConnectionFactory connectionFactory,
                                     NatsConsumerConfig natsConsumerConfig,
                                     DeserializationSchema<OutputT> payloadDeserializer,
                                     SourceReaderContext readerContext,
                                     String subject,
                                     Boundedness mode) {
        id = sourceId + "-" + Utils.generatePrefixedId(sourceId);
        this.connectionFactory = connectionFactory;
        this.payloadDeserializer = payloadDeserializer;
        this.readerContext = checkNotNull(readerContext);
        subbedSplits = new ArrayList<>();
        messages = new FutureCompletingBlockingQueue<>();
        this.config= natsConsumerConfig;
        this.subject = subject;
        this.mode = mode;
    }


    @Override
    public void start() {
        LOG.debug("{} | start", id);
        try {
            connection = connectionFactory.connect();
            js = connection.jetStream();
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .stream(config.getStreamName()).bind(true).durable(config.getConsumerName())
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
        InputStatus is;
        if (this.mode == Boundedness.CONTINUOUS_UNBOUNDED) {
            is = InputStatus.MORE_AVAILABLE;
        } else {
            is =  messages.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
        }
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
