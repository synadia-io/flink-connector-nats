// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia;

public interface Constants {
    String SOURCE_SUBJECTS = "source.subjects";
    String SOURCE_PAYLOAD_SERIALIZER = "source.payload.deserializer";
    String SOURCE_STARTUP_JITTER_MIN = "source.startup.jitter.min";
    String SOURCE_STARTUP_JITTER_MAX = "source.startup.jitter.max";

    String SINK_SUBJECTS = "sink.subjects";
    String SINK_PAYLOAD_SERIALIZER = "sink.payload.serializer";
    String SINK_STARTUP_JITTER_MIN = "sink.startup.jitter.min";
    String SINK_STARTUP_JITTER_MAX = "sink.startup.jitter.max";
}
