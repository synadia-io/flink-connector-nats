// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.reader;
import java.util.Map;
import java.util.function.Consumer;

import io.nats.client.*;
import io.synadia.flink.v0.source.NatsJetStreamSourceConfiguration;
import io.synadia.flink.v0.source.split.NatsSubjectSplit;
import io.synadia.flink.v0.utils.ConnectionContext;
import io.synadia.flink.v0.utils.ConnectionFactory;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static io.synadia.flink.v0.utils.MiscUtils.generatePrefixedId;

public class NatsSubjectSplitReader
        implements SplitReader<Message, NatsSubjectSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(NatsSubjectSplitReader.class);

    private final String id;
    private final NatsJetStreamSourceConfiguration sourceConfiguration;
    private NatsSubjectSplit registeredSplit;

    private final Map<String, ConnectionContext> connections;
    private JetStreamSubscription subscription;
    private final ConnectionFactory connectionFactory;

    public NatsSubjectSplitReader(String sourceId, Map<String, ConnectionContext> connections, NatsJetStreamSourceConfiguration sourceConfiguration, ConnectionFactory connectionFactory) {
        id = generatePrefixedId(sourceId);
        this.sourceConfiguration = sourceConfiguration;
        this.connections = connections;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public RecordsWithSplitIds<Message> fetch() throws IOException {
        RecordsBySplits.Builder<Message> builder = new RecordsBySplits.Builder<>();

        // Return when no split registered to this reader.
        if (registeredSplit == null) {
            return builder.build();
        }

        String splitId = registeredSplit.splitId();
        ConnectionContext ctx = getContext(splitId);

        try {
            // update the subscription for the split
            this.subscription = createSubscription(ctx, registeredSplit.getSubject());

            List<Message> messages = subscription.fetch(sourceConfiguration.getMaxFetchRecords(), sourceConfiguration.getFetchTimeout());
            messages.forEach(msg -> builder.add(splitId, msg));

            //Stop consuming if running in batch mode, and the configured size of messages is fetched
            if (sourceConfiguration.getBoundedness() == Boundedness.BOUNDED && messages.size() <= sourceConfiguration.getMaxFetchRecords()) {
                builder.addFinishedSplit(splitId);
            }
        }
        catch(Exception e) {
            throw new IOException(e); //Finish reading message from split if the consumer is deleted for any reason.
        }

        LOG.debug("{} | {} | Finished polling message {}", id, splitId, 1);
        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<NatsSubjectSplit> splitsChanges) {
        LOG.debug("{} | handleSplitsChanges {}", id, splitsChanges);

        // Get all the partition assignments and stopping offsets.
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        if (registeredSplit != null) {
            throw new IllegalStateException("This split reader have assigned split.");
        }

        List<NatsSubjectSplit> newSplits = splitsChanges.splits();
        this.registeredSplit = newSplits.get(0);

        ConnectionContext ctx = getContext(registeredSplit.splitId());

        try {
            // update the subscription for the split
            this.subscription = createSubscription(ctx, registeredSplit.getSubject());
        } catch (JetStreamApiException | IOException e) {
            throw new FlinkRuntimeException(e);
        }

        LOG.info("Register split {} consumer for current reader.", registeredSplit);
    }

    //TODO Check and implement expected behavior for NATS
    @Override
    public void pauseOrResumeSplits(Collection<NatsSubjectSplit> splitsToPause, Collection<NatsSubjectSplit> splitsToResume) {
        LOG.debug("{} | pauseOrResumeSplits {} | {}", id, splitsToPause, splitsToResume);
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public void close() throws Exception {
        unsubscribe();
    }

    private void unsubscribe() throws FlinkRuntimeException {
        if (subscription == null) {
            return;
        }

        try {
            subscription.unsubscribe();
        } catch (RuntimeException e) {
            throw new FlinkRuntimeException(e);
        } finally {
            subscription = null;
        }
    }

    /** Create a specified {@link Consumer} by the given topic partition. */
    private JetStreamSubscription createSubscription(ConnectionContext context, String subject) throws IOException, JetStreamApiException {
        PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                .durable(sourceConfiguration.getConsumerName())
                .build();
        return context.js.subscribe(subject, pullOptions);
    }

    private ConnectionContext getContext (String splitId) throws FlinkRuntimeException {
        ConnectionContext ctx = connections.getOrDefault(splitId, null);

        if (ctx == null || ctx.connection.getStatus().equals(Connection.Status.CLOSED) || ctx.connection.getStatus().equals(Connection.Status.DISCONNECTED)) {
            try {
                ctx = connectionFactory.connectContext();
                this.connections.put(splitId, ctx);

            } catch (IOException e) {
                throw new FlinkRuntimeException(e);
            }
        }

        return ctx;
    }
}
