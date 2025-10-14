// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink;

import io.nats.client.support.JsonUtils;
import io.synadia.flink.message.SinkConverter;
import io.synadia.flink.sink.writer.NatsSinkWriter;
import io.synadia.flink.utils.ConnectionFactory;
import io.synadia.flink.utils.YamlUtils;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.util.List;

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.synadia.flink.utils.Constants.SINK_CONVERTER_CLASS_NAME;
import static io.synadia.flink.utils.Constants.SUBJECTS;
import static io.synadia.flink.utils.MiscUtils.generateId;
import static io.synadia.flink.utils.MiscUtils.getClassName;

/**
 * A Flink Sink to publish data to one or more NATS subjects
 * @param <InputT> the type of object from the source to convert for publishing
 */
public class NatsSink<InputT> implements Sink<InputT> {
    protected final String id;
    protected final List<String> subjects;
    protected final SinkConverter<InputT> sinkConverter;
    protected final ConnectionFactory connectionFactory;

    protected NatsSink(List<String> subjects,
             SinkConverter<InputT> sinkConverter,
             ConnectionFactory connectionFactory)
    {
        id = generateId();
        this.subjects = subjects;
        this.sinkConverter = sinkConverter;
        this.connectionFactory = connectionFactory;
    }

    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, SINK_CONVERTER_CLASS_NAME, getClassName(sinkConverter));
        JsonUtils.addStrings(sb, SUBJECTS, subjects);
        return endJson(sb).toString();
    }

    public String toYaml() {
        StringBuilder sb = YamlUtils.beginYaml();
        YamlUtils.addField(sb, 0, SINK_CONVERTER_CLASS_NAME, getClassName(sinkConverter));
        YamlUtils.addStrings(sb, 0, SUBJECTS, subjects);
        return sb.toString();
    }

    public List<String> getSubjects() {
        return subjects;
    }

    @Override
    public SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        return new NatsSinkWriter<>(id, subjects, sinkConverter, connectionFactory, context);
    }

    @Override
    public String toString() {
        return "NatsSink{" +
            "id='" + id + '\'' +
            ", subjects=" + subjects +
            ", sinkConverter=" + sinkConverter.getClass().getCanonicalName() +
            ", connectionFactory=" + connectionFactory +
            '}';
    }
}
