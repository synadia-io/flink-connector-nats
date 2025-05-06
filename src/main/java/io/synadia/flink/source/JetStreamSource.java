// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.support.JsonValueUtils;
import io.synadia.flink.enumerator.NatsSourceEnumerator;
import io.synadia.flink.payload.PayloadDeserializer;
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
import static io.synadia.flink.utils.Constants.PAYLOAD_DESERIALIZER;
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
    public final PayloadDeserializer<OutputT> payloadDeserializer;
    public final ConnectionFactory connectionFactory;

    JetStreamSource(Boundedness boundedness,
                    Map<String, JetStreamSubjectConfiguration> configById,
                    PayloadDeserializer<OutputT> payloadDeserializer,
                    ConnectionFactory connectionFactory)
    {
        this.boundedness = boundedness;
        this.configById = Collections.unmodifiableMap(configById);
        this.payloadDeserializer = payloadDeserializer;
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
        return new JetStreamSourceReader<>(boundedness, payloadDeserializer, connectionFactory, readerContext);
    }

    @Override
    public TypeInformation<OutputT> getProducedType() {
        return payloadDeserializer.getProducedType();
    }

    @Override
    public String toString() {
        return "JetStreamSource{" +
            "payloadDeserializer=" + getClassName(payloadDeserializer) +
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
        bm.put(PAYLOAD_DESERIALIZER, getClassName(payloadDeserializer));
        bm.put(JETSTREAM_SUBJECT_CONFIGURATIONS, ba.jv);
        return bm.jv.toJson();
    }

    public String toYaml() {
        StringBuilder sb = YamlUtils.beginYaml();
        YamlUtils.addField(sb, 0, PAYLOAD_DESERIALIZER, getClassName(payloadDeserializer));
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
            && payloadDeserializer.getClass().equals(that.payloadDeserializer.getClass())
            && Objects.equals(connectionFactory, that.connectionFactory);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(boundedness);
        result = 31 * result + configById.hashCode();
        result = 31 * result + Objects.hashCode(payloadDeserializer.getClass());
        result = 31 * result + Objects.hashCode(connectionFactory);
        return result;
    }
}
