// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.support.JsonUtils;
import io.synadia.flink.enumerator.NatsSourceEnumerator;
import io.synadia.flink.message.SourceConverter;
import io.synadia.flink.source.reader.NatsSourceReader;
import io.synadia.flink.source.split.NatsSubjectCheckpointSerializer;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.source.split.NatsSubjectSplitSerializer;
import io.synadia.flink.utils.ConnectionFactory;
import io.synadia.flink.utils.YamlUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.synadia.flink.utils.Constants.*;
import static io.synadia.flink.utils.MiscUtils.generateId;
import static io.synadia.flink.utils.MiscUtils.getClassName;

/**
 * Flink Source to consume data from one or more NATS subjects
 * @param <OutputT> the type of object to convert message payload data to
 */
public class NatsSource<OutputT> implements
    Source<OutputT, NatsSubjectSplit, Collection<NatsSubjectSplit>>,
    ResultTypeQueryable<OutputT>
{
    /**
     * The source id
     */
    protected final String id;

    /**
     * Source-level configuration (boundedness, source queue capacity).
     * See {@link SourceConfig}.
     */
    public final SourceConfig config;

    /**
     * The list of subjects
     */
    protected final List<String> subjects;

    /**
     * The source converter
     */
    protected final SourceConverter<OutputT> sourceConverter;

    /**
     * The connection factory
     */
    protected final ConnectionFactory connectionFactory;

    /**
     * Construct the source
     *
     * @param config            the source-level configuration
     * @param subjects          the subject
     * @param sourceConverter   the source converter
     * @param connectionFactory the connection factory
     */
    protected NatsSource(SourceConfig config,
                         List<String> subjects,
                         SourceConverter<OutputT> sourceConverter,
                         ConnectionFactory connectionFactory)
    {
        id = generateId();
        this.config = config;
        this.subjects = subjects;
        this.sourceConverter = sourceConverter;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public Boundedness getBoundedness() {
        return config.boundedness;
    }

    /**
     * Get the list of subjects
     * @return the list
     */
    public List<String> getSubjects() {
        return subjects;
    }

    /**
     * Get the JSON representation of the source configuration
     * @return the JSON
     */
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, SOURCE_CONVERTER_CLASS_NAME, getClassName(sourceConverter));
        JsonUtils.addField(sb, SOURCE_QUEUE_CAPACITY, config.sourceQueueCapacity);
        JsonUtils.addStrings(sb, SUBJECTS, subjects);
        return endJson(sb).toString();
    }

    /**
     * Get the YAML representation of the source configuration
     * @return the YAML
     */
    public String toYaml() {
        StringBuilder sb = YamlUtils.beginYaml();
        YamlUtils.addField(sb, 0, SOURCE_CONVERTER_CLASS_NAME, getClassName(sourceConverter));
        YamlUtils.addField(sb, 0, SOURCE_QUEUE_CAPACITY, config.sourceQueueCapacity);
        YamlUtils.addStrings(sb, 0, SUBJECTS, subjects);
        return sb.toString();
    }

    @Override
    public SplitEnumerator<NatsSubjectSplit, Collection<NatsSubjectSplit>> createEnumerator(
        SplitEnumeratorContext<NatsSubjectSplit> enumContext) throws Exception
    {
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
        return new NatsSourceEnumerator<>(enumContext, checkpoint);
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
        return new NatsSourceReader<>(connectionFactory, sourceConverter, readerContext, config.sourceQueueCapacity);
    }

    @Override
    public TypeInformation<OutputT> getProducedType() {
        return sourceConverter.getProducedType();
    }

    @Override
    public String toString() {
        return "NatsSource{" +
            "id='" + id + '\'' +
            ", subjects=" + subjects +
            ", sourceConverter=" + sourceConverter.getClass().getCanonicalName() +
            ", connectionFactory=" + connectionFactory +
            '}';
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NatsSource)) return false;

        // id is intentionally excluded — it's a generated per-instance
        // identifier; two structurally-equal sources should still compare
        // equal even though their ids differ.
        NatsSource<?> that = (NatsSource<?>) o;
        return Objects.equals(config, that.config)
            && subjects.equals(that.subjects)
            && sourceConverter.getClass().equals(that.sourceConverter.getClass())
            && Objects.equals(connectionFactory, that.connectionFactory);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(config);
        result = 31 * result + subjects.hashCode();
        result = 31 * result + Objects.hashCode(sourceConverter.getClass());
        result = 31 * result + Objects.hashCode(connectionFactory);
        return result;
    }
}
