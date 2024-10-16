// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.reader;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.config.SourceConfiguration;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.source.split.NatsSubjectSplitState;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class NatsJetstreamSourceReader<OutputT> extends SourceReaderBase<
        Message, OutputT, NatsSubjectSplit, NatsSubjectSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(NatsJetstreamSourceReader.class);

    private final Connection connection;
    private final AtomicReference<Throwable> cursorCommitThrowable;
    @VisibleForTesting
    final SortedMap<Long, Map<String, List<Message>>> cursorsToCommit;
    private final ConcurrentMap<String, List<Message>> cursorsOfFinishedSplits;
    private final SourceConfiguration sourceConfiguration;

    public NatsJetstreamSourceReader(FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> elementsQueue,
                                     NatsSourceFetcherManager fetcherManager,
                                     PayloadDeserializer<OutputT> deserializationSchema,
                                     SourceConfiguration configuration,
                                     Connection connection,
                                     SourceReaderContext readerContext
    ) {
        super(elementsQueue, fetcherManager, new NatsRecordEmitter<>(deserializationSchema), configuration, readerContext);
        this.sourceConfiguration = configuration;
        this.connection = connection;
        this.cursorsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.cursorsOfFinishedSplits = new ConcurrentHashMap<>();
        this.cursorCommitThrowable = new AtomicReference<>();
    }


    @Override
    public void start() {
        super.start();
        if (sourceConfiguration.isEnableAutoAcknowledgeMessage()) {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

            scheduler.scheduleAtFixedRate(this::cumulativeAcknowledgmentMessage,
                    sourceConfiguration.getMaxFetchTime().toMillis(),
                    sourceConfiguration.getNatsAutoAckInterval().toMillis(),
                    TimeUnit.MILLISECONDS);
        }

    }

    @Override
    public InputStatus pollNext(ReaderOutput<OutputT> output) throws Exception {
        Throwable cause = cursorCommitThrowable.get();
        if (cause != null) {
            throw new FlinkRuntimeException("An error occurred in acknowledge message.", cause);
        }

        return super.pollNext(output);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("Committing cursors for checkpoint {}", checkpointId);
        Map<String, List<Message>> cursors = cursorsToCommit.get(checkpointId); //TODO convert string to Subject Class
        try {
            ((NatsSourceFetcherManager) splitFetcherManager).acknowledgeMessages(cursors);
            LOG.debug("Successfully acknowledge cursors for checkpoint {}", checkpointId);

            // Clean up the cursors.
            cursorsOfFinishedSplits.keySet().removeAll(cursors.keySet());
            cursorsToCommit.headMap(checkpointId + 1).clear();
        } catch (Exception e) {
            LOG.error("Failed to acknowledge cursors for checkpoint {}", checkpointId, e);
            cursorCommitThrowable.compareAndSet(null, e);
        }
    }

    @Override
    public List<NatsSubjectSplit> snapshotState(long checkpointId) {
        List<NatsSubjectSplit> splits = super.snapshotState(checkpointId);

        // Perform a snapshot for these splits.
        Map<String, List<Message>> cursors =
                cursorsToCommit.computeIfAbsent(checkpointId, id -> new HashMap<>());
        // Put the cursors of the active splits.
        for (NatsSubjectSplit split : splits) {
            cursors.put(split.getSubject(), split.getCurrentMessages());
        }
        // Put cursors of all the finished splits.
        cursors.putAll(cursorsOfFinishedSplits);

        return splits;
    }
    @Override
    public void close() throws Exception {
        //TODO Review this again and remove TODO
        super.close();
        connection.close();
    }

    @Override
    public void addSplits(List<NatsSubjectSplit> splits) {
        super.addSplits(splits);
    }

    @Override
    protected void onSplitFinished(Map<String, NatsSubjectSplitState> finishedSplitIds) {
// Close all the finished splits.
        for (String splitId : finishedSplitIds.keySet()) {
            ((NatsSourceFetcherManager) splitFetcherManager).closeFetcher(splitId);
        }

        // We don't require new splits, all the splits are pre-assigned by source enumerator.
        if (LOG.isDebugEnabled()) {
            LOG.debug("onSplitFinished event: {}", finishedSplitIds);
        }

        for (Map.Entry<String, NatsSubjectSplitState> entry : finishedSplitIds.entrySet()) {
            NatsSubjectSplitState state = entry.getValue();
            cursorsOfFinishedSplits.put(state.getSplit().splitId(), state.getSplit().getCurrentMessages());
        }
    }

    @Override
    protected NatsSubjectSplitState initializedState(NatsSubjectSplit natsSubjectSplit) {
        return new NatsSubjectSplitState(natsSubjectSplit);
    }

    @Override
    protected NatsSubjectSplit toSplitType(String s, NatsSubjectSplitState natsSubjectSplitState) {
        return natsSubjectSplitState.toNatsSubjectSplit();
    }

    private void cumulativeAcknowledgmentMessage() {
        Map<String, List<Message>> cursors = new HashMap<>(cursorsOfFinishedSplits);

        // We reuse snapshotState for acquiring a consume status snapshot.
        // So the checkpoint didn't really happen, so we just pass a fake checkpoint id.
        List<NatsSubjectSplit> splits = super.snapshotState(1L);
        for (NatsSubjectSplit split : splits) {
            cursors.put(split.getSubject(), split.getCurrentMessages());
        }

        try {
            ((NatsSourceFetcherManager) splitFetcherManager).acknowledgeMessages(cursors);
            // Clean up the finish splits.
            cursorsOfFinishedSplits.keySet().removeAll(cursors.keySet());
        } catch (Exception e) {
            LOG.error("Fail in auto cursor commit.", e);
            cursorCommitThrowable.compareAndSet(null, e);
        }
    }

    /** Factory method for creating NatsJetstreamSourceReader. */
    public static <OutputT> NatsJetstreamSourceReader<OutputT> create(
            SourceConfiguration sourceConfiguration,
            PayloadDeserializer<OutputT> deserializationSchema,
            SourceReaderContext readerContext)
            throws Exception {

        // Create a message queue with the predefined source option.
        int queueCapacity = sourceConfiguration.getMessageQueueCapacity();
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> elementsQueue =
                new FutureCompletingBlockingQueue<>(queueCapacity);

        Connection connection = Nats.connect(sourceConfiguration.getUrl());

        // Initialize the deserialization schema before creating the nats reader.

        // Create an ordered split reader supplier.
        Supplier<SplitReader<Message, NatsSubjectSplit>> splitReaderSupplier =
                () ->
                        new NatsSubjectSplitReader(
                                connection,
                                sourceConfiguration);

        NatsSourceFetcherManager fetcherManager =
                new NatsSourceFetcherManager(
                        elementsQueue, splitReaderSupplier, readerContext.getConfiguration());

        return new NatsJetstreamSourceReader<>(
                elementsQueue,
                fetcherManager,
                deserializationSchema,
                sourceConfiguration,
                connection,
                readerContext);
    }
}
