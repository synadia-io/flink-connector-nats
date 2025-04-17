// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink;

import io.synadia.flink.helpers.WordCount;
import io.synadia.flink.utils.MiscUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class UtilsTest {
    @Test
    void testTypeInfo() {
        TypeInformation<WordCount> ti = MiscUtils.getTypeInformation(WordCount.class);
        assertSame(ti.getTypeClass(), WordCount.class);
        assertFalse(ti.isBasicType());
        assertEquals(2, ti.getArity());
    }
}
