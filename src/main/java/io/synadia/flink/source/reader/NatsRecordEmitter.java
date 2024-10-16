package io.synadia.flink.source.reader;

import io.nats.client.Message;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.split.NatsSubjectSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;

public class NatsRecordEmitter<OutputT>
        implements RecordEmitter<Message, OutputT, NatsSubjectSplitState> {

    private final PayloadDeserializer<OutputT> deserializationSchema;
    private final SourceOutputWrapper<OutputT> sourceOutputWrapper;

    public NatsRecordEmitter(PayloadDeserializer<OutputT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        this.sourceOutputWrapper = new SourceOutputWrapper<>();
    }

    @Override
    public void emitRecord(
            Message element, SourceOutput<OutputT> output, NatsSubjectSplitState splitState)
            throws Exception {
        // Update the source output.
        sourceOutputWrapper.setSourceOutput(output);
        sourceOutputWrapper.setTimestamp(element);

        // Deserialize the message and since it to output.
        deserializationSchema.getObject(splitState.getSplit().getSubject(),element.getData(), null);
        splitState.getSplit().getCurrentMessages().add(element);

        // Release the messages if we use message pool in Pulsar.
        // TODO Check NATS Client once if there is any reference of releasing a message from pool.
        //element.release();
    }

    private static class SourceOutputWrapper<OutputT> implements Collector<OutputT> {

        private SourceOutput<OutputT> sourceOutput;
        private long timestamp;

        @Override
        public void collect(OutputT record) {
            if (timestamp > 0) {
                sourceOutput.collect(record, timestamp);
            } else {
                sourceOutput.collect(record);
            }
        }

        @Override
        public void close() {
            // Nothing to do here.
        }

        private void setSourceOutput(SourceOutput<OutputT> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        /**
         * Set the event timestamp.
         */
        private void setTimestamp(Message message) {
            this.timestamp = message.metaData().timestamp().toInstant().toEpochMilli();
        }
    }
}

