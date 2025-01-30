// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.io.synadia.flink.v0;

import io.nats.client.ConsumeOptions;
import io.nats.client.FetchConsumeOptions;
import io.synadia.flink.v0.source.ManagedConsumeOptions;
import io.synadia.io.synadia.flink.TestBase;
import org.apache.flink.api.connector.source.Boundedness;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ManagedObjectsTests extends TestBase {

    @Test
    public void testManagedConsumeOptions() throws Exception {
        ManagedConsumeOptions mco = new ManagedConsumeOptions(Boundedness.BOUNDED);
        assertEquals(Boundedness.BOUNDED, mco.getBoundedness());
        assertEquals(FetchConsumeOptions.DEFAULT_FETCH_OPTIONS, mco.getBoundedOptions());
        assertNull(mco.getUnboundedOptions());

        mco = new ManagedConsumeOptions(Boundedness.CONTINUOUS_UNBOUNDED);
        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, mco.getBoundedness());
        assertEquals(ConsumeOptions.DEFAULT_CONSUME_OPTIONS, mco.getUnboundedOptions());
        assertNull(mco.getBoundedOptions());

        FetchConsumeOptions fco = FetchConsumeOptions.builder()
            .maxMessages(1000)
            .maxBytes(10000)
            .expiresIn(5000)
            .build();
        mco = new ManagedConsumeOptions(fco);
        assertEquals(Boundedness.BOUNDED, mco.getBoundedness());
        assertNull(mco.getUnboundedOptions());
        FetchConsumeOptions mcoFco = mco.getBoundedOptions();
        assertEquals(1000, mcoFco.getMaxMessages());
        assertEquals(10000, mcoFco.getMaxBytes());
        assertEquals(5000, mcoFco.getExpiresInMillis());

        mco = (ManagedConsumeOptions)javaSerializeDeserializeObject(mco);
        assertEquals(Boundedness.BOUNDED, mco.getBoundedness());
        assertNull(mco.getUnboundedOptions());
        mcoFco = mco.getBoundedOptions();
        assertEquals(1000, mcoFco.getMaxMessages());
        assertEquals(10000, mcoFco.getMaxBytes());
        assertEquals(5000, mcoFco.getExpiresInMillis());

        ConsumeOptions co = ConsumeOptions.builder()
            .batchSize(1000)
            .expiresIn(5000)
            .build();
        mco = new ManagedConsumeOptions(co);
        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, mco.getBoundedness());
        assertNull(mco.getBoundedOptions());
        ConsumeOptions mcoCo = mco.getUnboundedOptions();
        assertEquals(1000, mcoCo.getBatchSize());
        assertEquals(5000, mcoCo.getExpiresInMillis());

        mco = (ManagedConsumeOptions)javaSerializeDeserializeObject(mco);
        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, mco.getBoundedness());
        assertNull(mco.getBoundedOptions());
        mcoCo = mco.getUnboundedOptions();
        assertEquals(1000, mcoCo.getBatchSize());
        assertEquals(5000, mcoCo.getExpiresInMillis());
    }
}
