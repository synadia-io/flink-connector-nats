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
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class NatsSourceReader<OutputT> implements SourceReader<OutputT, NatsSubjectSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(NatsSourceReader.class);

    private final ConnectionFactory connectionFactory;
    private final PayloadDeserializer<OutputT> payloadDeserializer;
    private final SourceReaderContext readerContext;
    private final List<NatsSubjectSplit> subbedSplits;
    private final LinkedBlockingQueue<Message> messages;
    private CompletableFuture<Void> availabilityFuture;
    private Connection connection;
    private Dispatcher dispatcher;

    public NatsSourceReader(ConnectionFactory connectionFactory,
                            PayloadDeserializer<OutputT> payloadDeserializer,
                            SourceReaderContext readerContext) {
        this.connectionFactory = connectionFactory;
        this.payloadDeserializer = payloadDeserializer;
        this.readerContext = checkNotNull(readerContext);
        this.availabilityFuture = new CompletableFuture<>();
        subbedSplits = new ArrayList<>();
        messages = new LinkedBlockingQueue<>();
    }

    @Override
    public void start() {
        LOG.debug("start");
        try {
            connection = connectionFactory.connect();
            dispatcher = connection.createDispatcher(messages::put);
        }
        catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OutputT> output) throws Exception {
        LOG.debug("pollNext");
        if (availabilityFuture == null) {
            // force isAvailable() to be called first to evaluate rate-limiting
            return InputStatus.NOTHING_AVAILABLE;
        }
        // reset future because the next record may hit the rate limit
        availabilityFuture = null;
        Message m = messages.poll();
        if (m == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        output.collect(payloadDeserializer.getObject(m.getData()));
        return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<NatsSubjectSplit> snapshotState(long checkpointId) {
        LOG.debug("snapshotState");
        return Collections.unmodifiableList(subbedSplits);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        LOG.debug("isAvailable " + availabilityFuture);
        return availabilityFuture;
    }

    @Override
    public void addSplits(List<NatsSubjectSplit> splits) {
        LOG.debug("addSplits " + splits);
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
        LOG.debug("notifyNoMoreSplits");
        // set availability so that pollNext is actually called
        availabilityFuture.complete(null);
    }

    @Override
    public void close() throws Exception {
        LOG.debug("close");
        connection.close();
    }
}
