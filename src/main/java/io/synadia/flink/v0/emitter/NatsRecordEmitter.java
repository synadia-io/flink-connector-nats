package io.synadia.flink.v0.emitter;

import io.nats.client.Message;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.split.NatsSubjectSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class NatsRecordEmitter<OutputT>
        implements RecordEmitter<Message, OutputT, NatsSubjectSplitState> {

    private final PayloadDeserializer<OutputT> payloadDeserializer;

    public NatsRecordEmitter(PayloadDeserializer<OutputT> payloadDeserializer) {
        this.payloadDeserializer = payloadDeserializer;
    }

    @Override
    public void emitRecord(Message element,
                           SourceOutput<OutputT> output,
                           NatsSubjectSplitState splitState)
            throws Exception {
        // Deserialize the message and since it to output.
        output.collect(payloadDeserializer.getObject(splitState.getSplit().getSubject(), element.getData(), element.getHeaders()));
        splitState.getSplit().getCurrentMessages().add(element);
    }
}

