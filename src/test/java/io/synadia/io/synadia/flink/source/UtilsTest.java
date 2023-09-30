package io.synadia.io.synadia.flink.source;

import io.synadia.flink.Utils;
import io.synadia.io.synadia.flink.WordCount;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class UtilsTest {
    @Test
    void testTypeInfo() {
        TypeInformation<WordCount> ti = Utils.getTypeInformation(WordCount.class);
        assertSame(ti.getTypeClass(), WordCount.class);
        assertFalse(ti.isBasicType());
        assertEquals(2, ti.getArity());
    }
}
