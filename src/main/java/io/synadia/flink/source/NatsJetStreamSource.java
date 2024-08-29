// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.NUID;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.config.SourceConfiguration;
import io.synadia.flink.source.enumerator.NatsSourceEnumerator;
import io.synadia.flink.source.reader.NatsJetstreamSourceReader;
import io.synadia.flink.source.split.NatsSubjectCheckpointSerializer;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.source.split.NatsSubjectSplitSerializer;
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

    private PayloadDeserializer<OutputT> deserializationSchema;
    private static final Logger LOG = LoggerFactory.getLogger(NatsJetStreamSource.class);
    private SourceConfiguration sourceConfiguration;


    // Package-private constructor to ensure usage of the Builder for object creation
    NatsJetStreamSource(PayloadDeserializer<OutputT> deserializationSchema, SourceConfiguration sourceConfiguration) {
        this.deserializationSchema = deserializationSchema;
        this.sourceConfiguration = sourceConfiguration;
    }

    @Override
    public TypeInformation<OutputT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public Boundedness getBoundedness() {
        return null;
    }

    @Override
    public SplitEnumerator<NatsSubjectSplit, Collection<NatsSubjectSplit>> createEnumerator(
            SplitEnumeratorContext<NatsSubjectSplit> enumContext) throws Exception {
        List<NatsSubjectSplit> list = new ArrayList<>();
        list.add(new NatsSubjectSplit(sourceConfiguration.getSubjectName()));
        return restoreEnumerator(enumContext, list);
    }

    @Override
    public SplitEnumerator<NatsSubjectSplit, Collection<NatsSubjectSplit>> restoreEnumerator(
            SplitEnumeratorContext<NatsSubjectSplit> enumContext, Collection<NatsSubjectSplit> checkpoint)
            throws Exception {
        return new NatsSourceEnumerator(NUID.nextGlobal(), enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<NatsSubjectSplit> getSplitSerializer() {
        return new NatsSubjectSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<NatsSubjectSplit>> getEnumeratorCheckpointSerializer() {
        return new NatsSubjectCheckpointSerializer();
    }

    @Override
    public SourceReader<OutputT, NatsSubjectSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return NatsJetstreamSourceReader.create(sourceConfiguration, deserializationSchema, readerContext);
    }

}
