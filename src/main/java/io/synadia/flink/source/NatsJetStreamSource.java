package io.synadia.flink.source;

import io.synadia.flink.Utils;
import io.synadia.flink.common.ConnectionFactory;
import io.synadia.flink.source.enumerator.NatsSourceEnumerator;
import io.synadia.flink.source.split.NatsSubjectCheckpointSerializer;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.source.split.NatsSubjectSplitSerializer;
import io.synadia.flink.payload.PayloadDeserializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NatsJetStreamSource<OutputT> implements Source<OutputT, NatsSubjectSplit, Collection<NatsSubjectSplit>>, ResultTypeQueryable<OutputT> {

    private final ConnectionFactory connectionFactory;
    private final String natsSubject;
    private final PayloadDeserializer<OutputT> payloadDeserializer;
    private final NatsConsumeOptions config;
    private static final Logger LOG = LoggerFactory.getLogger(NatsJetStreamSource.class);
    private final String id;
    private final Boundedness mode;

    // Package-private constructor to ensure usage of the Builder for object creation
    NatsJetStreamSource(PayloadDeserializer<OutputT> payloadDeserializer, ConnectionFactory connectionFactory, String natsSubject, NatsConsumeOptions config, Boundedness mode) {
        id = Utils.generateId();
        this.payloadDeserializer = payloadDeserializer;
        this.connectionFactory = connectionFactory;
        this.natsSubject = natsSubject;
        this.config = config;
        this.mode = mode;
    }

    @Override
    public TypeInformation<OutputT> getProducedType() {
        return payloadDeserializer.getProducedType();
    }

    @Override
    public Boundedness getBoundedness() {
        return this.mode;
    }

    @Override
    public SplitEnumerator<NatsSubjectSplit, Collection<NatsSubjectSplit>> createEnumerator(
            SplitEnumeratorContext<NatsSubjectSplit> enumContext) throws Exception {
        LOG.debug("{} | createEnumerator", id);
        List<NatsSubjectSplit> list = new ArrayList<>();
        list.add(new NatsSubjectSplit(natsSubject));
        return restoreEnumerator(enumContext, list);
    }

    @Override
    public SplitEnumerator<NatsSubjectSplit, Collection<NatsSubjectSplit>> restoreEnumerator(
            SplitEnumeratorContext<NatsSubjectSplit> enumContext, Collection<NatsSubjectSplit> checkpoint)
            throws Exception {
        LOG.debug("{} | restoreEnumerator", id);
        return new NatsSourceEnumerator(id, enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<NatsSubjectSplit> getSplitSerializer() {
        LOG.debug("{} | getSplitSerializer", id);
        return new NatsSubjectSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<NatsSubjectSplit>> getEnumeratorCheckpointSerializer() {
        LOG.debug("{} | getEnumeratorCheckpointSerializer", id);
        return new NatsSubjectCheckpointSerializer();
    }

    @Override
    public SourceReader<OutputT, NatsSubjectSplit> createReader(SourceReaderContext readerContext) throws Exception {
        LOG.debug("{} | createReader", id);
        return new NatsJetStreamSourceReader<>(id, connectionFactory, config, payloadDeserializer, readerContext, natsSubject, mode);
    }

    @Override
    public String toString() {
        return "NatsJetStreamSource{" +
                "id='" + id + '\'' +
                ", subjects=" + natsSubject +
                ", payloadDeserializer=" + payloadDeserializer.getClass().getCanonicalName() +
                ", connectionFactory=" + connectionFactory +
                '}';
    }
}
