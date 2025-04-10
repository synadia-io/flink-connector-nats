// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.reader;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.utils.ConnectionFactory;
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

import static org.apache.flink.util.Preconditions.checkNotNull;

public class NatsSourceReader<OutputT> implements SourceReader<OutputT, NatsSubjectSplit> {
    private final ConnectionFactory connectionFactory;
    private final PayloadDeserializer<OutputT> payloadDeserializer;
    private final List<NatsSubjectSplit> subbedSplits;
    private final FutureCompletingBlockingQueue<Message> messages;
    private Connection connection;
    private Dispatcher dispatcher;

    public NatsSourceReader(ConnectionFactory connectionFactory,
                            PayloadDeserializer<OutputT> payloadDeserializer,
                            SourceReaderContext readerContext) {
        this.connectionFactory = connectionFactory;
        this.payloadDeserializer = payloadDeserializer;
        checkNotNull(readerContext); // it's not used but is supposed to be provided
        subbedSplits = new ArrayList<>();
        messages = new FutureCompletingBlockingQueue<>();
    }

    @Override
    public void start() {
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
            return InputStatus.NOTHING_AVAILABLE;
        }
        output.collect(payloadDeserializer.getObject(m));
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
        for (NatsSubjectSplit split : splits) {
            int ix = subbedSplits.indexOf(split);
            if (ix == -1) {
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
        connection.close();
    }

    @Override
    public String toString() {
        return "NatsSourceReader{" +
            ", subbedSplits=" + subbedSplits +
            '}';
    }
}
