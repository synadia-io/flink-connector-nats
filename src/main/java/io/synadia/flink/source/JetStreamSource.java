// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.synadia.flink.enumerator.NatsSourceEnumerator;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.reader.JetStreamSourceReader;
import io.synadia.flink.source.split.JetStreamCheckpointSerializer;
import io.synadia.flink.source.split.JetStreamSplit;
import io.synadia.flink.source.split.JetStreamSplitSerializer;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static io.synadia.flink.utils.MiscUtils.generateId;

/**
 * Flink Source to consume data from one or more NATS subjects
 * @param <OutputT> the type of object to convert message payload data to
 */
public class JetStreamSource<OutputT> implements
    Source<OutputT, JetStreamSplit, Collection<JetStreamSplit>>,
    ResultTypeQueryable<OutputT>
{
    protected final String id;
    protected final PayloadDeserializer<OutputT> payloadDeserializer;
    protected final Boundedness boundedness;
    protected final Map<String, JetStreamSubjectConfiguration> configById;
    protected final ConnectionFactory connectionFactory;
    protected final Configuration configuration;

    @Override
    public String toString() {
        return "JetStreamSource{" +
            "id=" + id + '\'' +
            ", payloadDeserializer=" + payloadDeserializer.getClass().getSimpleName() +
            ", configById=" + configById +
            ", connectionFactory=" + connectionFactory +
            ", configuration=" + configuration +
            '}';
    }

    public JetStreamSource(PayloadDeserializer<OutputT> payloadDeserializer,
                           Boundedness boundedness,
                           Map<String, JetStreamSubjectConfiguration> configById,
                           ConnectionFactory connectionFactory,
                           Configuration configuration)
    {
        id = generateId();
        this.payloadDeserializer = payloadDeserializer;
        this.boundedness = boundedness;
        this.configById = configById;
        this.connectionFactory = connectionFactory;
        this.configuration = configuration;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SplitEnumerator<JetStreamSplit, Collection<JetStreamSplit>> createEnumerator(
        SplitEnumeratorContext<JetStreamSplit> enumContext) throws Exception
    {
        List<JetStreamSplit> list = new ArrayList<>();
        for (JetStreamSubjectConfiguration mcc : configById.values()) {
            list.add(new JetStreamSplit(mcc));
        }
        return restoreEnumerator(enumContext, list);
    }

    @Override
    public SplitEnumerator<JetStreamSplit, Collection<JetStreamSplit>> restoreEnumerator(
        SplitEnumeratorContext<JetStreamSplit> enumContext,
        Collection<JetStreamSplit> checkpoint)
    {
        return new NatsSourceEnumerator<>(id, enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<JetStreamSplit> getSplitSerializer() {
        return new JetStreamSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<JetStreamSplit>> getEnumeratorCheckpointSerializer() {
        return new JetStreamCheckpointSerializer();
    }

    @Override
    public SourceReader<OutputT, JetStreamSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new JetStreamSourceReader<>(id, boundedness, connectionFactory, payloadDeserializer, readerContext);
    }

    @Override
    public TypeInformation<OutputT> getProducedType() {
        return payloadDeserializer.getProducedType();
    }
}
