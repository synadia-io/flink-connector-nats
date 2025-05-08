// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.support.JsonValueUtils;
import io.synadia.flink.enumerator.NatsSourceEnumerator;
import io.synadia.flink.message.SourceConverter;
import io.synadia.flink.source.reader.JetStreamSourceReader;
import io.synadia.flink.source.split.JetStreamCheckpointSerializer;
import io.synadia.flink.source.split.JetStreamSplit;
import io.synadia.flink.source.split.JetStreamSplitSerializer;
import io.synadia.flink.utils.ConnectionFactory;
import io.synadia.flink.utils.YamlUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.*;

import static io.synadia.flink.utils.Constants.JETSTREAM_SUBJECT_CONFIGURATIONS;
import static io.synadia.flink.utils.Constants.SOURCE_CONVERTER_CLASS_NAME;
import static io.synadia.flink.utils.MiscUtils.getClassName;

/**
 * Flink Source to consume data from one or more NATS subjects
 * @param <OutputT> the type of object to convert message payload data to
 */
public class JetStreamSource<OutputT> implements
    Source<OutputT, JetStreamSplit, Collection<JetStreamSplit>>,
    ResultTypeQueryable<OutputT>
{
    public final Boundedness boundedness;
    public final Map<String, JetStreamSubjectConfiguration> configById;
    public final SourceConverter<OutputT> sourceConverter;
    public final ConnectionFactory connectionFactory;

    JetStreamSource(Boundedness boundedness,
                    Map<String, JetStreamSubjectConfiguration> configById,
                    SourceConverter<OutputT> sourceConverter,
                    ConnectionFactory connectionFactory)
    {
        this.boundedness = boundedness;
        this.configById = Collections.unmodifiableMap(configById);
        this.sourceConverter = sourceConverter;
        this.connectionFactory = connectionFactory;
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
        return new NatsSourceEnumerator<>(enumContext, checkpoint);
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
        return new JetStreamSourceReader<>(boundedness, sourceConverter, connectionFactory, readerContext);
    }

    @Override
    public TypeInformation<OutputT> getProducedType() {
        return sourceConverter.getProducedType();
    }

    @Override
    public String toString() {
        return "JetStreamSource{" +
            "sourceConverter=" + getClassName(sourceConverter) +
            ", configById=" + configById +
            ", connectionFactory=" + connectionFactory +
            '}';
    }

    public String toJson() {
        JsonValueUtils.ArrayBuilder ba = JsonValueUtils.arrayBuilder();
        for (String id : configById.keySet()) {
            ba.add(configById.get(id).toJsonValue());
        }
        JsonValueUtils.MapBuilder bm = JsonValueUtils.mapBuilder();
        bm.put(SOURCE_CONVERTER_CLASS_NAME, getClassName(sourceConverter));
        bm.put(JETSTREAM_SUBJECT_CONFIGURATIONS, ba.jv);
        return bm.jv.toJson();
    }

    public String toYaml() {
        StringBuilder sb = YamlUtils.beginYaml();
        YamlUtils.addField(sb, 0, SOURCE_CONVERTER_CLASS_NAME, getClassName(sourceConverter));
        YamlUtils.addField(sb, 0, JETSTREAM_SUBJECT_CONFIGURATIONS);
        for (String id : configById.keySet()) {
            sb.append(configById.get(id).toYaml(1));
        }
        return sb.toString();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JetStreamSource)) return false;

        JetStreamSource<?> that = (JetStreamSource<?>) o;
        return boundedness == that.boundedness
            && configById.equals(that.configById)
            && sourceConverter.getClass().equals(that.sourceConverter.getClass())
            && Objects.equals(connectionFactory, that.connectionFactory);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(boundedness);
        result = 31 * result + configById.hashCode();
        result = 31 * result + Objects.hashCode(sourceConverter.getClass());
        result = 31 * result + Objects.hashCode(connectionFactory);
        return result;
    }
}
