// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.reader;

import io.nats.client.Message;
import io.synadia.flink.v0.emitter.NatsRecordEmitter;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.NatsJetStreamSourceConfiguration;
import io.synadia.flink.v0.source.split.NatsSubjectSplit;
import io.synadia.flink.v0.source.split.NatsSubjectSplitState;
import io.synadia.flink.v0.utils.ConnectionFactory;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.synadia.flink.v0.utils.MiscUtils.generatePrefixedId;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class NatsJetStreamSourceReader<OutputT>
    extends SourceReaderBase<Message, OutputT, NatsSubjectSplit, NatsSubjectSplitState>
{
    private static final Logger LOG = LoggerFactory.getLogger(NatsJetStreamSourceReader.class);

    private final String id;
    private final ConnectionFactory connectionFactory;
    private final SourceReaderContext readerContext;
    private final AtomicReference<Throwable> cursorCommitThrowable;
    final SortedMap<Long, Map<String, List<Message>>> cursorsToCommit;
    private final ConcurrentMap<String, List<Message>> cursorsOfFinishedSplits;
    private final NatsJetStreamSourceConfiguration sourceConfiguration;

    public NatsJetStreamSourceReader(String sourceId,
                                     FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> elementsQueue,
                                     NatsSourceFetcherManager fetcherManager,
                                     NatsJetStreamSourceConfiguration sourceConfiguration,
                                     ConnectionFactory connectionFactory,
                                     PayloadDeserializer<OutputT> payloadDeserializer,
                                     SourceReaderContext readerContext
    ) {
        super(elementsQueue,
            fetcherManager,
            new NatsRecordEmitter<>(payloadDeserializer),
            sourceConfiguration.getConfiguration(), readerContext);
        id = generatePrefixedId(sourceId);
        this.sourceConfiguration = sourceConfiguration;
        this.connectionFactory = connectionFactory;
        this.readerContext = checkNotNull(readerContext);
        this.cursorsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.cursorsOfFinishedSplits = new ConcurrentHashMap<>();
        this.cursorCommitThrowable = new AtomicReference<>();
    }

    @Override
    public void start() {
        LOG.debug("{} | start", id);
        super.start();
        if (sourceConfiguration.isEnableAutoAcknowledgeMessage()) {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(this::cumulativeAcknowledgmentMessage,
                    sourceConfiguration.getFetchTimeout().toMillis(),
                    sourceConfiguration.getAutoAckInterval().toMillis(),
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
        //TODO convert string to Subject Class
        Map<String, List<Message>> cursors = cursorsToCommit.get(checkpointId);
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
            ((NatsSourceFetcherManager) splitFetcherManager)
                .acknowledgeMessages(cursors);
            // Clean up the finish splits.
            cursorsOfFinishedSplits.keySet().removeAll(cursors.keySet());
        } catch (Exception e) {
            LOG.error("Fail in auto cursor commit.", e);
            cursorCommitThrowable.compareAndSet(null, e);
        }
    }
}
