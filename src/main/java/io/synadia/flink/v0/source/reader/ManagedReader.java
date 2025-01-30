// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.reader;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.synadia.flink.v0.payload.MessageRecord;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.split.ManagedSplit;
import io.synadia.flink.v0.utils.ConnectionFactory;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.synadia.flink.v0.utils.MiscUtils.generatePrefixedId;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class ManagedReader<OutputT> implements SourceReader<OutputT, ManagedSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(ManagedReader.class);

    private final String id;
    private final ConnectionFactory connectionFactory;
    private final PayloadDeserializer<OutputT> payloadDeserializer;
    private final SourceReaderContext readerContext;
    private final List<ManagedSplit> subbedSplits;
    private final FutureCompletingBlockingQueue<Message> messages;
    private Connection connection;
    private Dispatcher dispatcher;

    public ManagedReader(String sourceId,
                         ConnectionFactory connectionFactory,
                         PayloadDeserializer<OutputT> payloadDeserializer,
                         SourceReaderContext readerContext) {
        id = generatePrefixedId(sourceId);
        this.connectionFactory = connectionFactory;
        this.payloadDeserializer = payloadDeserializer;
        this.readerContext = checkNotNull(readerContext);
        subbedSplits = new ArrayList<>();
        messages = new FutureCompletingBlockingQueue<>();
    }

    @Override
    public void start() {
        LOG.debug("{} | start", id);
        try {
            connection = connectionFactory.connect();
            dispatcher = connection.createDispatcher(m -> messages.put(1, m));
        }
        catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OutputT> output) throws Exception {
        Message m = messages.poll();
        if (m == null) {
            LOG.debug("{} | pollNext no message NOTHING_AVAILABLE", id);
            return InputStatus.NOTHING_AVAILABLE;
        }
        output.collect(payloadDeserializer.getObject(new MessageRecord(m)));
        InputStatus is = messages.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
        LOG.debug("{} | pollNext had message, then {}", id, is);
        return is;
    }

    @Override
    public List<ManagedSplit> snapshotState(long checkpointId) {
        LOG.debug("{} | snapshotState", id);
        return Collections.unmodifiableList(subbedSplits);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return messages.getAvailabilityFuture();
    }

    @Override
    public void addSplits(List<ManagedSplit> splits) {
        for (ManagedSplit split : splits) {
            LOG.debug("{} | addSplits {}", id, split);
            int ix = subbedSplits.indexOf(split);
            if (ix == -1) {
//                dispatcher.subscribe(split.getSubject());
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
            connection.close();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        LOG.debug("{} | handleSourceEvents {}", id, sourceEvent);
    }

    @Override
    public String toString() {
        return "NatsSourceReader{" +
            "id='" + id + '\'' +
            ", subbedSplits=" + subbedSplits +
            '}';
    }
}
