// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source;

import io.nats.client.Message;
import io.synadia.flink.utils.ConnectionFactory;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.reader.NatsJetStreamSourceReader;
import io.synadia.flink.v0.source.reader.NatsSourceFetcherManager;
import io.synadia.flink.v0.source.reader.NatsSubjectSplitReader;
import io.synadia.flink.v0.source.split.NatsSubjectSplit;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.List;
import java.util.function.Supplier;

import static io.synadia.flink.utils.MiscUtils.generateId;

public class NatsJetStreamSource<OutputT> extends NatsSource<OutputT> {
    protected final String id;
    private final NatsJetStreamSourceConfiguration sourceConfiguration;

    NatsJetStreamSource(PayloadDeserializer<OutputT> payloadDeserializer, ConnectionFactory connectionFactory, List<String> subjects,
                        NatsJetStreamSourceConfiguration sourceConfiguration) {
        super(payloadDeserializer, connectionFactory, subjects, NatsJetStreamSource.class);
        id = generateId();
        this.sourceConfiguration = sourceConfiguration;
    }

    @Override
    public Boundedness getBoundedness() {
        logger.debug("{} | Boundedness", id);
        return sourceConfiguration.getBoundedness(); // TODO this varies from NatsSource, understand why
    }

    @Override
    public SourceReader<OutputT, NatsSubjectSplit> createReader(SourceReaderContext readerContext) throws Exception {
        int queueCapacity = sourceConfiguration.getMessageQueueCapacity();
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> elementsQueue =
            new FutureCompletingBlockingQueue<>(queueCapacity);

        Supplier<SplitReader<Message, NatsSubjectSplit>> splitReaderSupplier =
            () -> new NatsSubjectSplitReader(id, connectionFactory, sourceConfiguration);

        NatsSourceFetcherManager fetcherManager =
            new NatsSourceFetcherManager(
                elementsQueue, splitReaderSupplier, readerContext.getConfiguration());

        return new NatsJetStreamSourceReader<>(
            id,
            elementsQueue,
            fetcherManager,
            sourceConfiguration, connectionFactory, payloadDeserializer,
            readerContext);
    }
}

