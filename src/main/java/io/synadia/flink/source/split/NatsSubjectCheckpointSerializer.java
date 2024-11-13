/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, MiscUtils 2.0 (the
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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Serializes and deserializes the {@link NatsSubjectSplit}. This class needs to handle
 * deserializing splits from older versions.
 */
@Internal
public class NatsSubjectCheckpointSerializer implements SimpleVersionedSerializer<Collection<NatsSubjectSplit>> {

    public static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Collection<NatsSubjectSplit> splits) throws IOException {
        int startSize = 4; // account for first value number of splits
        for (NatsSubjectSplit split : splits) {
            startSize += split.splitId().length();
        }
        final DataOutputSerializer out = new DataOutputSerializer(startSize);
        out.writeInt(splits.size());
        for (NatsSubjectSplit split : splits) {
            NatsSubjectSplitSerializer.serializeV1(out, split);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public Collection<NatsSubjectSplit> deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unrecognized version: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final int num = in.readInt();
        final ArrayList<NatsSubjectSplit> result = new ArrayList<>(num);
        for (int x = 0; x < num; x++) {
            result.add(NatsSubjectSplitSerializer.deserializeV1(in));
        }
        return result;
    }
}
