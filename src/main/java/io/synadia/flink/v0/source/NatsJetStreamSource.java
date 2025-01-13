// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source;

import io.nats.client.Message;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.reader.NatsJetStreamSourceReader;
import io.synadia.flink.v0.source.reader.NatsSourceFetcherManager;
import io.synadia.flink.v0.source.reader.NatsSubjectSplitReader;
import io.synadia.flink.v0.source.split.NatsSubjectSplit;
import io.synadia.flink.v0.utils.ConnectionContext;
import io.synadia.flink.v0.utils.ConnectionFactory;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static io.synadia.flink.v0.utils.MiscUtils.generateId;

public class NatsJetStreamSource<OutputT> extends NatsSource<OutputT> {
    protected final String id;
    private final NatsJetStreamSourceConfiguration sourceConfiguration;
    private final Map<String, ConnectionContext> connections;

    NatsJetStreamSource(PayloadDeserializer<OutputT> payloadDeserializer, ConnectionFactory connectionFactory, List<String> subjects,
                        NatsJetStreamSourceConfiguration sourceConfiguration) {
        super(payloadDeserializer, connectionFactory, subjects, NatsJetStreamSource.class);

        id = generateId();
        this.sourceConfiguration = sourceConfiguration;
        this.connections = new ConcurrentHashMap<>();
    }

    @Override
    public Boundedness getBoundedness() {
        logger.debug("{} | Boundedness", id);
        return sourceConfiguration.getBoundedness(); // TODO this varies from NatsSource, understand why
    }

    @Override
    public SourceReader<OutputT, NatsSubjectSplit> createReader(SourceReaderContext readerContext) throws Exception {
        int queueCapacity = sourceConfiguration.getMessageQueueCapacity();

        FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> elementsQueue = new FutureCompletingBlockingQueue<>(queueCapacity);
        NatsSourceFetcherManager fetcherManager = getNatsSourceFetcherManager(readerContext, elementsQueue);

        return new NatsJetStreamSourceReader<>(id, elementsQueue, fetcherManager, sourceConfiguration, connections, payloadDeserializer, readerContext);
    }

    private NatsSourceFetcherManager getNatsSourceFetcherManager(SourceReaderContext readerContext, FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> elementsQueue) throws FlinkRuntimeException {
        Supplier<SplitReader<Message, NatsSubjectSplit>> splitReaderSupplier = () ->
                new NatsSubjectSplitReader(id, connections, sourceConfiguration, connectionFactory);
        return new NatsSourceFetcherManager(elementsQueue, splitReaderSupplier, readerContext.getConfiguration());
    }
}
