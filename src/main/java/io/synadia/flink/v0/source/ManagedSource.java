// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source;

import io.synadia.flink.v0.enumerator.NatsSourceEnumerator;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.reader.ManagedReader;
import io.synadia.flink.v0.source.split.ManagedCheckpointSerializer;
import io.synadia.flink.v0.source.split.ManagedSplit;
import io.synadia.flink.v0.source.split.ManagedSplitSerializer;
import io.synadia.flink.v0.utils.ConnectionFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.synadia.flink.v0.utils.MiscUtils.generateId;

/**
 * Flink Source to consume data from one or more NATS subjects
 * @param <OutputT> the type of object to convert message payload data to
 */
public class ManagedSource<OutputT> implements
    Source<OutputT, ManagedSplit, Collection<ManagedSplit>>,
    ResultTypeQueryable<OutputT>
{
    private static final Logger LOG = LoggerFactory.getLogger(ManagedSource.class);

    protected final String id;
    protected final PayloadDeserializer<OutputT> payloadDeserializer;
    protected final ManagedSourceConfiguration sourceConfig;

    protected final ConnectionFactory connectionFactory;

    ManagedSource(PayloadDeserializer<OutputT> payloadDeserializer,
                  ConnectionFactory connectionFactory,
                  ManagedSourceConfiguration sourceConfig)
    {
        id = generateId();
        this.sourceConfig = sourceConfig;
        this.payloadDeserializer = payloadDeserializer;
        this.connectionFactory = connectionFactory;
        LOG.debug("{} | init {}", id, sourceConfig);
    }

    @Override
    public Boundedness getBoundedness() {
        return sourceConfig.defaultManagedConsumeOptions.getBoundedness();
    }

    @Override
    public SplitEnumerator<ManagedSplit, Collection<ManagedSplit>> createEnumerator(
        SplitEnumeratorContext<ManagedSplit> enumContext) throws Exception
    {
        LOG.debug("{} | createEnumerator", id);
        List<ManagedSplit> list = new ArrayList<>();
        for (ManagedSubjectConfiguration mcc : sourceConfig.configById.values()) {
            list.add(new ManagedSplit(mcc));
        }
        return restoreEnumerator(enumContext, list);
    }

    @Override
    public SplitEnumerator<ManagedSplit, Collection<ManagedSplit>> restoreEnumerator(
        SplitEnumeratorContext<ManagedSplit> enumContext,
        Collection<ManagedSplit> checkpoint)
    {
        LOG.debug("{} | restoreEnumerator", id);
        return new NatsSourceEnumerator<>(id, enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<ManagedSplit> getSplitSerializer() {
        LOG.debug("{} | getSplitSerializer", id);
        return new ManagedSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<ManagedSplit>> getEnumeratorCheckpointSerializer() {
        LOG.debug("{} | getEnumeratorCheckpointSerializer", id);
        return new ManagedCheckpointSerializer();
    }

    @Override
    public SourceReader<OutputT, ManagedSplit> createReader(SourceReaderContext readerContext) throws Exception {
        LOG.debug("{} | createReader", id);
        return new ManagedReader<>(id, connectionFactory, payloadDeserializer, readerContext);
    }

    @Override
    public TypeInformation<OutputT> getProducedType() {
        return payloadDeserializer.getProducedType();
    }
}
