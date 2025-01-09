// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.io.synadia.flink;

import io.synadia.flink.v0.utils.PropertiesUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class UtilsTest {
    @Test
    void testTypeInfo() {
        TypeInformation<WordCount> ti = PropertiesUtils.getTypeInformation(WordCount.class);
        assertSame(ti.getTypeClass(), WordCount.class);
        assertFalse(ti.isBasicType());
        assertEquals(2, ti.getArity());
    }
}
