// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import io.nats.client.support.Validator;
import io.synadia.payload.PayloadSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static io.synadia.Constants.SINK_SUBJECTS;

public class NatsSinkBuilder<InputT> {
    private List<String> subjects;
    private Properties sinkProperties;
    private PayloadSerializer<InputT> payloadSerializer;

    public NatsSinkBuilder<InputT> subjects(String... subjects) {
        this.subjects = subjects == null || subjects.length == 0 ? null : Arrays.asList(subjects);
        return this;
    }

    public NatsSinkBuilder<InputT> subjects(List<String> subjects) {
        if (subjects == null || subjects.isEmpty()) {
            this.subjects = null;
        }
        else {
            this.subjects = new ArrayList<>(subjects);
        }
        return this;
    }

    public NatsSinkBuilder<InputT> sinkProperties(Properties sinkProperties) {
        this.sinkProperties = sinkProperties;
        return this;
    }

    public NatsSinkBuilder<InputT> payloadSerializer(PayloadSerializer<InputT> payloadSerializer) {
        this.payloadSerializer = payloadSerializer;
        return this;
    }

    public NatsSink<InputT> build() {
        Validator.required(sinkProperties, "Sink Properties");
        Validator.required(payloadSerializer, "Payload Serializer");
        if (subjects == null && sinkProperties != null) {
            String temp = sinkProperties.getProperty(SINK_SUBJECTS);
            if (temp != null) {
                subjects = Arrays.asList(temp.split(","));
            }
            Validator.required(subjects, "Sink Subjects");
        }
        return new NatsSink<>(subjects, sinkProperties, payloadSerializer);
    }
}
