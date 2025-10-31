// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.reader;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.synadia.flink.message.SourceConverter;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class NatsSourceReader<OutputT> implements SourceReader<OutputT, NatsSubjectSplit> {
    private final ConnectionFactory connectionFactory;
    private final SourceConverter<OutputT> sourceConverter;
    private final List<NatsSubjectSplit> subbedSplits;
    private final FutureCompletingBlockingQueue<Message> messages;
    private final ReentrantLock connectionLock;

    private Connection _connection;
    private Dispatcher dispatcher;

    /**
     * Construct the Nats Srouce Reader
     * @param connectionFactory the connection factory
     * @param sourceConverter the source converter
     * @param readerContext the context
     */
    public NatsSourceReader(ConnectionFactory connectionFactory,
                            SourceConverter<OutputT> sourceConverter,
                            SourceReaderContext readerContext) {
        this.connectionFactory = connectionFactory;
        this.sourceConverter = sourceConverter;
        checkNotNull(readerContext); // it's not used but is supposed to be provided
        subbedSplits = new ArrayList<>();
        messages = new FutureCompletingBlockingQueue<>();
        connectionLock = new ReentrantLock();
    }

    @Override
    public void start() {
        getConnection();
    }

    private Connection getConnection() {
        connectionLock.lock();
        try {
            if (_connection == null) {
                try {
                    _connection = connectionFactory.connect();
                    dispatcher = _connection.createDispatcher(m -> messages.put(1, m));
                }
                catch (IOException e) {
                    throw new FlinkRuntimeException(e);
                }
            }
            return _connection;
        }
        finally {
            connectionLock.unlock();
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OutputT> output) throws Exception {
        Message m = messages.poll();
        if (m == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        output.collect(sourceConverter.convert(m));
        return messages.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<NatsSubjectSplit> snapshotState(long checkpointId) {
        return Collections.unmodifiableList(subbedSplits);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return messages.getAvailabilityFuture();
    }

    @Override
    public void addSplits(List<NatsSubjectSplit> splits) {
        Connection connection = null;
        for (NatsSubjectSplit split : splits) {
            int ix = subbedSplits.indexOf(split);
            if (ix == -1) {
                if (connection == null) {
                    connection = getConnection();
                }
                dispatcher.subscribe(split.getSubject());
                subbedSplits.add(split);
            }
        }
    }

    @Override
    public void notifyNoMoreSplits() {
    }

    @Override
    public void close() throws Exception {
        connectionLock.lock();
        try {
            if (_connection != null) {
                _connection.close();
            }
        }
        finally {
            _connection = null;
            connectionLock.unlock();
        }
    }

    @Override
    public String toString() {
        return "NatsSourceReader{" +
            ", subbedSplits=" + subbedSplits +
            '}';
    }
}
