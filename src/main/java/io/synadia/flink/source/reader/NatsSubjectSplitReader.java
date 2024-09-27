package io.synadia.flink.source.reader;

import io.nats.client.Connection;
import io.nats.client.Consumer;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.synadia.flink.source.config.SourceConfiguration;
import io.synadia.flink.source.split.NatsSubjectSplit;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NatsSubjectSplitReader
        implements SplitReader<Message, NatsSubjectSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(NatsSubjectSplitReader.class);

    private final Connection connection;
    private final SourceConfiguration sourceConfiguration;
    private JetStreamSubscription jetStreamSubscription;
    private NatsSubjectSplit registeredSplit;


    public NatsSubjectSplitReader(
            Connection connection,
            SourceConfiguration sourceConfiguration) {
        this.connection = connection;
        this.sourceConfiguration = sourceConfiguration;
    }

    @Override
    @SuppressWarnings("java:S135")
    public RecordsWithSplitIds<Message> fetch() throws IOException {
        RecordsBySplits.Builder<Message> builder = new RecordsBySplits.Builder<>();

        // Return when no split registered to this reader.
        if (connection == null || registeredSplit == null) {
            return builder.build();
        }

        String splitId = registeredSplit.splitId();
        Deadline deadline = Deadline.fromNow(sourceConfiguration.getMaxFetchTime());

        for (int messageNum = 0;
             messageNum < sourceConfiguration.getMaxFetchRecords() && deadline.hasTimeLeft();
             messageNum++) {
            try {
                Duration fetchTime = sourceConfiguration.getFetchOneMessageTime();
                if (fetchTime == null) {
                    fetchTime = Duration.ofMillis(deadline.timeLeftIfAny().toMillis());
                }

                List<Message> messages = jetStreamSubscription.fetch(1, fetchTime);
                if (messages.isEmpty()) {
                    builder.addFinishedSplit(splitId);
                    break;
                }

                builder.add(splitId, messages.get(0));

                LOG.info("Finished polling message {}", 1);
                break; //TODO remove this

            } catch (TimeoutException e) {
                break;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<NatsSubjectSplit> splitsChanges) {
        LOG.debug("Handle split changes {}", splitsChanges);

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
            this.jetStreamSubscription = createJetstreamSubscription(registeredSplit.getSubject());
        } catch (Exception e) { //TODO Change to NATS Exception
            throw new FlinkRuntimeException(e);
        }

        LOG.info("Register split {} consumer for current reader.", registeredSplit);
    }

    //TODO Check and implement expected behavior for NATS
//    @Override
//    public void pauseOrResumeSplits(
//            Collection<NatsSubjectSplit> splitsToPause,
//            Collection<NatsSubjectSplit> splitsToResume) {
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
//    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {
        if (jetStreamSubscription != null) {
            jetStreamSubscription.unsubscribe(); //TODO this will delete consumer if its created through library, be careful
        }
    }

    public void notifyCheckpointComplete(String subject, List<Message> messages)
            throws Exception { //TODO Throw nats exception
        if (jetStreamSubscription == null) {
            this.jetStreamSubscription = createJetstreamSubscription(subject);
        }
        //Handle specially for cumulative ack
        if (jetStreamSubscription.getConsumerInfo().getConsumerConfiguration().getAckPolicy() == AckPolicy.All)
        {
            List<Message> reversed = Lists.reverse(messages);
            reversed.get(0).ack();
        }else {
            messages.forEach(message -> message.ack());
        }
    }

    // --------------------------- Helper Methods -----------------------------

    /** Create a specified {@link Consumer} by the given topic partition. */
    private JetStreamSubscription createJetstreamSubscription(String subject)
            throws Exception {
        PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                .durable(sourceConfiguration.getConsumerName())

                .build();
        JetStreamSubscription subscription = connection.jetStream().subscribe(subject, pullOptions);
        return subscription;
    }

}
