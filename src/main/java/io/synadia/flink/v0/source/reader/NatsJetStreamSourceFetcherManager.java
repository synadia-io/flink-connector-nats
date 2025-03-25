// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.reader;

import io.nats.client.Message;
import io.synadia.flink.source.reader.NatsSubjectSplitReader;
import io.synadia.flink.source.split.NatsSubjectSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

public class NatsJetStreamSourceFetcherManager
    extends SplitFetcherManager<Message, NatsSubjectSplit>
    implements Supplier<SplitReader<Message, NatsSubjectSplit>>
{
    private static final Logger LOG = LoggerFactory.getLogger(NatsJetStreamSourceFetcherManager.class);

    private final Map<String, Integer> splitFetcherMapping = new HashMap<>();
    private final Map<Integer, Boolean> fetcherStatus = new HashMap<>();

    /**
     * Creates a new SplitFetcherManager with multiple I/O threads.
     *
     * @param elementsQueue The queue that is used to hand over data from the I/O thread (the
     *     fetchers) to the reader, which emits the records and book-keeps the state. This must be
     *     the same queue instance that is also passed to the {@link SourceReaderBase}.
     * @param splitReaderSupplier The factory for the split reader that connects to the source
     * @param configuration The configuration object
     */
    public NatsJetStreamSourceFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Message>> elementsQueue,
            Supplier<SplitReader<Message, NatsSubjectSplit>> splitReaderSupplier,
            Configuration configuration) {
        super(elementsQueue, splitReaderSupplier, configuration);
    }

    @Override
    public SplitReader<Message, NatsSubjectSplit> get() {
        return null;
    }

    /**
     * Override this method for supporting multiple thread fetching, one fetcher thread for one
     * split.
     */
    @Override
    public void addSplits(List<NatsSubjectSplit> splitsToAdd) {
        for (NatsSubjectSplit split : splitsToAdd) {
            SplitFetcher<Message, NatsSubjectSplit> fetcher =
                    getOrCreateFetcher(split.splitId());
            fetcher.addSplits(singletonList(split));
            // This method could be executed multiple times.
            startFetcher(fetcher);
        }
    }

    // @Override // to keep compatible with Flink 1.17
    public void removeSplits(List<NatsSubjectSplit> splitsToRemove) {
        // TODO empty - wait for FLINK-31748 to implement it.
    }

    @Override
    protected void startFetcher(SplitFetcher<Message, NatsSubjectSplit> fetcher) {
        if (fetcherStatus.get(fetcher.fetcherId()) != Boolean.TRUE) {
            fetcherStatus.put(fetcher.fetcherId(), true);
            super.startFetcher(fetcher);
        }
    }

    /** Close the finished split related fetcher. */
    void closeFetcher(String splitId) {
        Integer fetchId = splitFetcherMapping.remove(splitId);
        if (fetchId != null) {
            fetcherStatus.remove(fetchId);
            SplitFetcher<Message, NatsSubjectSplit> fetcher = fetchers.remove(fetchId);
            if (fetcher != null) {
                fetcher.shutdown();
            }
        }
    }

    public void acknowledgeMessages(Map<String, List<Message>> cursorsToCommit)
            throws Exception { //TODO Change to nats exception
        LOG.debug("Acknowledge messages {}", cursorsToCommit);

        for (Map.Entry<String, List<Message>> entry : cursorsToCommit.entrySet()) {
            LOG.debug("No of messages to ack: {}", entry.getValue().size());

            String splitId = entry.getKey();
            SplitFetcher<Message, NatsSubjectSplit> fetcher = getOrCreateFetcher(splitId);
            triggerAcknowledge(fetcher, splitId, entry.getValue());
        }
    }

    private void triggerAcknowledge(
            SplitFetcher<Message, NatsSubjectSplit> splitFetcher,
            String splitId,
            List<Message> messages)
            throws Exception { //TODO Change to nats specific exception
        NatsSubjectSplitReader splitReader =
                (NatsSubjectSplitReader) splitFetcher.getSplitReader();
        splitReader.notifyCheckpointComplete(splitId, messages);
        startFetcher(splitFetcher);
    }

    private SplitFetcher<Message, NatsSubjectSplit> getOrCreateFetcher(String splitId) {
        SplitFetcher<Message, NatsSubjectSplit> fetcher;
        Integer fetcherId = splitFetcherMapping.get(splitId);

        if (fetcherId == null) {
            fetcher = createSplitFetcher();
        } else {
            fetcher = fetchers.get(fetcherId);
            // This fetcher has been stopped.
            if (fetcher == null) {
                fetcherStatus.remove(fetcherId);
                fetcher = createSplitFetcher();
            }
        }
        splitFetcherMapping.put(splitId, fetcher.fetcherId());

        return fetcher;
    }
}
