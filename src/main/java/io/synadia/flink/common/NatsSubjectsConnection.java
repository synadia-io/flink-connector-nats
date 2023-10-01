// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.common;

import java.io.Serializable;
import java.util.List;

public class NatsSubjectsConnection implements Serializable {
    protected final List<String> subjects;
    protected final ConnectionFactory connectionFactory;

    protected NatsSubjectsConnection(List<String> subjects, ConnectionFactory connectionFactory) {
        this.subjects = subjects;
        this.connectionFactory = connectionFactory;
    }
}
