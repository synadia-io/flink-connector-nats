package io.synadia.flink.source;

import io.nats.client.Connection;
import io.synadia.flink.Utils;
import io.synadia.flink.common.ConnectionFactory;
import io.synadia.flink.source.enumerator.NatsSourceEnumerator;
import io.synadia.flink.source.split.NatsSubjectCheckpointSerializer;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.source.split.NatsSubjectSplitSerializer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NATSJetstreamSource<T> implements Source<T, NatsSubjectSplit, Collection<NatsSubjectSplit>>, ResultTypeQueryable<T> {

    private final ConnectionFactory connectionFactory;
    private final String natsSubject;
    private final DeserializationSchema<T> deserializationSchema;
    private final NATSConsumerConfig config;
    private static final Logger LOG = LoggerFactory.getLogger(NATSJetstreamSource.class);
    private final String id;

    // Package-private constructor to ensure usage of the Builder for object creation
    NATSJetstreamSource(DeserializationSchema<T> deserializationSchema, ConnectionFactory connectionFactory, String natsSubject, NATSConsumerConfig config) {
        id = Utils.generateId();
        this.deserializationSchema = deserializationSchema;
        this.connectionFactory = connectionFactory;
        this.natsSubject = natsSubject;
        this.config = config;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.deserializationSchema.getProducedType();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
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
    public SourceReader<T, NatsSubjectSplit> createReader(SourceReaderContext readerContext) throws Exception {
        LOG.debug("{} | createReader", id);
        return new NatsJetstreamSourceReader<>(id, connectionFactory, config, deserializationSchema, readerContext, natsSubject);
    }

    @Override
    public String toString() {
        return "NatsSource{" +
                "id='" + id + '\'' +
                ", subjects=" + natsSubject +
                ", payloadDeserializer=" + deserializationSchema.getClass().getCanonicalName() +
                ", connectionFactory=" + connectionFactory +
                '}';
    }
}