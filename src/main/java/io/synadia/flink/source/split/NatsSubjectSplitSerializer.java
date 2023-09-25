/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.synadia.flink.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.VersionMismatchException;

import java.io.*;

/**
 * Serializes and deserializes the {@link NatsSubjectSplit}. This class needs to handle
 * deserializing splits from older versions.
 */
@Internal
public class NatsSubjectSplitSerializer implements SimpleVersionedSerializer<NatsSubjectSplit> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(NatsSubjectSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos))
        {
            out.writeUTF(split.getSubject());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public NatsSubjectSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            if (version != getVersion()) {
                throw new VersionMismatchException(
                        "Trying to deserialize NatsSubjectSplit serialized with unsupported version "
                                + version
                                + ". Version of serializer is "
                                + getVersion());
            }

            return new NatsSubjectSplit(in.readUTF());
        }
    }
}
