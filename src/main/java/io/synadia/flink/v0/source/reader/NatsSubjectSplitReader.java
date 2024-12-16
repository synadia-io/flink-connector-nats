package io.synadia.flink.v0.source.reader;

import io.nats.client.*;
import io.synadia.flink.utils.ConnectionFactory;
import io.synadia.flink.v0.NatsJetStreamSourceConfiguration;
import io.synadia.flink.v0.source.split.NatsSubjectSplit;
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

import static io.synadia.flink.utils.MiscUtils.generatePrefixedId;

public class NatsSubjectSplitReader
        implements SplitReader<Message, NatsSubjectSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(NatsSubjectSplitReader.class);

    private final String id;
    private final ConnectionFactory connectionFactory;
    private final NatsJetStreamSourceConfiguration sourceConfiguration;
    private JetStreamSubscription jetStreamSubscription;
    private NatsSubjectSplit registeredSplit;
    private Connection _connection; // lazy init from the factory

    public NatsSubjectSplitReader(String sourceId,
            ConnectionFactory connectionFactory,
            NatsJetStreamSourceConfiguration sourceConfiguration) {
        id = generatePrefixedId(sourceId);
        this.connectionFactory = connectionFactory;
        this.sourceConfiguration = sourceConfiguration;
    }

    @Override
    @SuppressWarnings("java:S135")
    public RecordsWithSplitIds<Message> fetch() throws IOException {
        RecordsBySplits.Builder<Message> builder = new RecordsBySplits.Builder<>();

        // Return when no split registered to this reader.
        if (getConnection() == null || registeredSplit == null) {
            return builder.build();
        }

        String splitId = registeredSplit.splitId();
        try {
            List<Message> messages = jetStreamSubscription.fetch(sourceConfiguration.getMaxFetchRecords(), sourceConfiguration.getFetchTimeout());
            messages.forEach((msg) -> {
                builder.add(splitId, msg);
            });
            //Stop consuming if running in batch mode and configured size of messages are fetched
            if (sourceConfiguration.getBoundedness() == Boundedness.BOUNDED && messages.size() <= sourceConfiguration.getMaxFetchRecords()) {
                builder.addFinishedSplit(splitId);
            }
        }
        catch(Exception e) {
            LOG.error(e.getMessage(), e);
            builder.addFinishedSplit(splitId); //Finish reading message from split if consumer is deleted for any reason.
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

        try {
            this.jetStreamSubscription = createSubscription(registeredSplit.getSubject());
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }

        LOG.info("Register split {} consumer for current reader.", registeredSplit);
    }

    //TODO Check and implement expected behavior for NATS
    @Override
    public void pauseOrResumeSplits(
            Collection<NatsSubjectSplit> splitsToPause,
            Collection<NatsSubjectSplit> splitsToResume)
    {
        LOG.debug("{} | pauseOrResumeSplits {} | {}", splitsToPause, splitsToResume);
//        // This shouldn't happen but just in case...
//        Preconditions.checkState(
//                splitsToPause.size() + splitsToResume.size() <= 1,
//                "This pulsar split reader only supports one split.");
//
//        if (!splitsToPause.isEmpty()) {
//            pulsarConsumer.pause();
//        } else if (!splitsToResume.isEmpty()) {
//            pulsarConsumer.resume();
//        }
    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {
        unsubscribe();
        closeConnection();
    }

    public void notifyCheckpointComplete(String subject, List<Message> messages)
            throws Exception { //TODO Throw nats exception
        //Handle specially for cumulative ack
        messages.forEach((msg)->{
                    getConnection().publish(msg.getReplyTo(),"+ACK".getBytes());
                }
        );

    }

    // --------------------------- Helper Methods -----------------------------

    private Connection getConnection() {
        if (_connection == null) {
            try {
                _connection = connectionFactory.connect();
            }
            catch (IOException e) {
                throw new FlinkRuntimeException(e);
            }
        }
        return _connection;
    }

    private void closeConnection() {
        if (_connection != null) {
            try {
                _connection.close();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException(e);
            }
            finally {
                _connection = null;
            }
        }
    }

    private void unsubscribe() {
        if (jetStreamSubscription != null) {
            try {
                jetStreamSubscription.unsubscribe();
            }
            catch (RuntimeException e) {
                throw new FlinkRuntimeException(e);
            }
            finally {
                jetStreamSubscription = null;
            }
        }
    }

    /** Create a specified {@link Consumer} by the given topic partition. */
    private JetStreamSubscription createSubscription(String subject) throws IOException, JetStreamApiException {
        PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                .durable(sourceConfiguration.getConsumerName())
                .build();
        return getConnection().jetStream().subscribe(subject, pullOptions);
    }

}
