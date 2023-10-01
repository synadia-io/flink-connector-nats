// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.source;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.synadia.flink.common.ConnectionFactory;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.split.NatsSubjectSplit;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class NatsSourceReader<OutputT> implements SourceReader<OutputT, NatsSubjectSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(NatsSourceReader.class);
    private static final AtomicInteger ID_MAKER = new AtomicInteger();

    private final int id;
    private final ConnectionFactory connectionFactory;
    private final PayloadDeserializer<OutputT> payloadDeserializer;
    private final SourceReaderContext readerContext;
    private final List<NatsSubjectSplit> subbedSplits;
    private final LinkedBlockingQueue<Message> messages;
    private CompletableFuture<Void> availabilityFuture;
    private AtomicReference<CountDownLatch> availabilityLatch;
    private Connection connection;
    private Dispatcher dispatcher;

    public NatsSourceReader(ConnectionFactory connectionFactory,
                            PayloadDeserializer<OutputT> payloadDeserializer,
                            SourceReaderContext readerContext) {
        id = ID_MAKER.incrementAndGet();
        this.connectionFactory = connectionFactory;
        this.payloadDeserializer = payloadDeserializer;
        this.readerContext = checkNotNull(readerContext);
        this.availabilityLatch = new AtomicReference<>();
        subbedSplits = new ArrayList<>();
        messages = new LinkedBlockingQueue<>();
    }

    @Override
    public void start() {
        LOG.debug(id + " | start");
        try {
            availabilityLatch.set(new CountDownLatch(1));
            connection = connectionFactory.connect();
            dispatcher = connection.createDispatcher(m -> {
//                LOG.debug(id + " | Message Received " + m.getSubject() + " | " + Debug.dataToString(m.getData()));
                messages.add(m);
                availabilityLatch.get().countDown();
            });
        }
        catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OutputT> output) throws Exception {
        Message m = messages.poll();
        if (m == null) {
            LOG.debug(id + " | pollNext no message NOTHING_AVAILABLE");
            return InputStatus.NOTHING_AVAILABLE;
        }
        output.collect(payloadDeserializer.getObject(m.getData()));
        InputStatus is = messages.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
        LOG.debug(id + " | pollNext had message then " + is);
        return is;
    }

    @Override
    public List<NatsSubjectSplit> snapshotState(long checkpointId) {
        LOG.debug(id + " | snapshotState");
        return Collections.unmodifiableList(subbedSplits);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        if (availabilityFuture == null || availabilityFuture.isDone()) {
            LOG.debug(id + " | isAvailable making new");
            availabilityFuture = CompletableFuture.runAsync(() -> {
                try {
                    availabilityLatch.get().await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        else {
            LOG.debug(id + " | isAvailable exists");
        }
        return availabilityFuture;
    }

    @Override
    public void addSplits(List<NatsSubjectSplit> splits) {
        for (NatsSubjectSplit split : splits) {
            LOG.debug(id + " | addSplits " + split);
            int ix = subbedSplits.indexOf(split);
            if (ix == -1) {
                dispatcher.subscribe(split.getSubject());
                subbedSplits.add(split);
            }
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.debug(id + " | notifyNoMoreSplits");
    }

    @Override
    public void close() throws Exception {
        LOG.debug(id + " | close");
        connection.close();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        LOG.debug(id + " | handleSourceEvents " + sourceEvent);
    }
}
