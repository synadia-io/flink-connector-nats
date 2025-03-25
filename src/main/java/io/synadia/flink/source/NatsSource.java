// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.synadia.flink.enumerator.NatsSourceEnumerator;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.reader.NatsSourceReader;
import io.synadia.flink.source.split.NatsSubjectCheckpointSerializer;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.source.split.NatsSubjectSplitSerializer;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.synadia.flink.utils.MiscUtils.generateId;

/**
 * Flink Source to consume data from one or more NATS subjects
 * @param <OutputT> the type of object to convert message payload data to
 */
public class NatsSource<OutputT> implements
    Source<OutputT, NatsSubjectSplit, Collection<NatsSubjectSplit>>,
    ResultTypeQueryable<OutputT>
{
    protected final String id;
    protected final List<String> subjects;
    protected final PayloadDeserializer<OutputT> payloadDeserializer;
    protected final ConnectionFactory connectionFactory;
    protected final Logger logger;

    NatsSource(PayloadDeserializer<OutputT> payloadDeserializer,
               ConnectionFactory connectionFactory,
               List<String> subjects)
    {
        this(payloadDeserializer, connectionFactory, subjects, NatsSource.class);
    }

    protected NatsSource(PayloadDeserializer<OutputT> payloadDeserializer,
                         ConnectionFactory connectionFactory,
                         List<String> subjects,
                         Class<?> logClazz)
    {
        id = generateId();
        this.subjects = subjects;
        this.payloadDeserializer = payloadDeserializer;
        this.connectionFactory = connectionFactory;
        logger = LoggerFactory.getLogger(logClazz.getCanonicalName()); // done this way to get full package path - needed to
        logger.debug("{} | Init {}", id, subjects);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<NatsSubjectSplit, Collection<NatsSubjectSplit>> createEnumerator(
        SplitEnumeratorContext<NatsSubjectSplit> enumContext) throws Exception
    {
        logger.debug("{} | createEnumerator", id);
        List<NatsSubjectSplit> list = new ArrayList<>();
        for (String subject : subjects) {
            list.add(new NatsSubjectSplit(subject));
        }
        return restoreEnumerator(enumContext, list);
    }

    @Override
    public SplitEnumerator<NatsSubjectSplit, Collection<NatsSubjectSplit>> restoreEnumerator(
        SplitEnumeratorContext<NatsSubjectSplit> enumContext,
        Collection<NatsSubjectSplit> checkpoint)
    {
        logger.debug("{} | restoreEnumerator", id);
        return new NatsSourceEnumerator<>(id, enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<NatsSubjectSplit> getSplitSerializer() {
        logger.debug("{} | getSplitSerializer", id);
        return new NatsSubjectSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<NatsSubjectSplit>> getEnumeratorCheckpointSerializer() {
        logger.debug("{} | getEnumeratorCheckpointSerializer", id);
        return new NatsSubjectCheckpointSerializer();
    }

    @Override
    public SourceReader<OutputT, NatsSubjectSplit> createReader(SourceReaderContext readerContext) throws Exception {
        logger.debug("{} | createReader", id);
        return new NatsSourceReader<>(id, connectionFactory, payloadDeserializer, readerContext);
    }

    @Override
    public TypeInformation<OutputT> getProducedType() {
        return payloadDeserializer.getProducedType();
    }

    @Override
    public String toString() {
        return "NatsSource{" +
            "id='" + id + '\'' +
            ", subjects=" + subjects +
            ", payloadDeserializer=" + payloadDeserializer.getClass().getCanonicalName() +
            ", connectionFactory=" + connectionFactory +
            '}';
    }
}
