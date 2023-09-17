// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.source;

import io.synadia.common.NatsSubjectsConnection;
import io.synadia.payload.PayloadDeserializer;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Flink Source to consumer data from one or more NATS subjects
 * @param <OutputT> the type of object to convert message payload data to
 */
public class NatsSource<OutputT> extends NatsSubjectsConnection implements Source<OutputT, NatsSubjectSourceSplit, Collection<NatsSubjectSourceSplit>> {
    private final PayloadDeserializer<OutputT> payloadDeserializer;

    NatsSource(List<String> subjects,
               Properties connectionProperties,
               String connectionPropertiesFile,
               long minConnectionJitter, long maxConnectionJitter,
               PayloadDeserializer<OutputT> payloadDeserializer
    ) {
        super(subjects, connectionProperties, connectionPropertiesFile, minConnectionJitter, maxConnectionJitter);
        this.payloadDeserializer = payloadDeserializer;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<NatsSubjectSourceSplit, Collection<NatsSubjectSourceSplit>> createEnumerator(SplitEnumeratorContext<NatsSubjectSourceSplit> enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<NatsSubjectSourceSplit, Collection<NatsSubjectSourceSplit>> restoreEnumerator(SplitEnumeratorContext<NatsSubjectSourceSplit> enumContext, Collection<NatsSubjectSourceSplit> checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<NatsSubjectSourceSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<Collection<NatsSubjectSourceSplit>> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public SourceReader<OutputT, NatsSubjectSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return null;
    }

    /**
     * Get the payload deserializer registered for this source
     * @return the deserializer
     */
    public PayloadDeserializer<OutputT> getPayloadDeserializer() {
        return payloadDeserializer;
    }
}
