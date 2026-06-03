// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

import io.nats.client.support.JsonParseException;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Coverage for the {@link BuilderBase.ConfigurationAdapter} factories
 * exposed by {@link BuilderBase}. Exercises both shapes —
 * {@code jsonAdapter} and {@code yamlAdapter} — across the three
 * accessors: {@code getList}, {@code getString}, {@code getInt}.
 *
 * <p>The {@code getInt} accessor has no internal caller (it's there for
 * future int-valued config fields), so this is the only place where it's
 * actually invoked.</p>
 */
class BuilderBaseTest {

    // ===== jsonAdapter =====

    @Test
    void jsonAdapter_getList() {
        BuilderBase.ConfigurationAdapter a = BuilderBase.jsonAdapter(json(
            "{\"subjects\":[\"a\",\"b\"],\"empty\":[]}"));

        assertEquals(Arrays.asList("a", "b"), a.getList("subjects"));
        assertEquals(Collections.emptyList(), a.getList("empty"));
        assertEquals(Collections.emptyList(), a.getList("missing"));
    }

    @Test
    void jsonAdapter_getString() {
        BuilderBase.ConfigurationAdapter a = BuilderBase.jsonAdapter(json(
            "{\"name\":\"orders\"}"));

        assertEquals("orders", a.getString("name"));
        assertNull(a.getString("missing"));
    }

    @Test
    void jsonAdapter_getInt() {
        BuilderBase.ConfigurationAdapter a = BuilderBase.jsonAdapter(json(
            "{\"count\":42,\"zero\":0,\"negative\":-7}"));

        assertEquals(42, a.getInt("count", -1));
        assertEquals(0, a.getInt("zero", -1));
        assertEquals(-7, a.getInt("negative", -1));
        assertEquals(99, a.getInt("missing", 99));
    }

    // ===== yamlAdapter =====

    @Test
    void yamlAdapter_getList() {
        BuilderBase.ConfigurationAdapter a = BuilderBase.yamlAdapter(yaml(
            "subjects:\n  - a\n  - b\nempty: []\n"));

        assertEquals(Arrays.asList("a", "b"), a.getList("subjects"));
        assertEquals(Collections.emptyList(), a.getList("empty"));
        assertEquals(Collections.emptyList(), a.getList("missing"));
    }

    @Test
    void yamlAdapter_getString() {
        BuilderBase.ConfigurationAdapter a = BuilderBase.yamlAdapter(yaml("name: orders\n"));

        assertEquals("orders", a.getString("name"));
        assertNull(a.getString("missing"));
    }

    @Test
    void yamlAdapter_getInt() {
        BuilderBase.ConfigurationAdapter a = BuilderBase.yamlAdapter(yaml(
            "count: 42\nzero: 0\nnegative: -7\n"));

        assertEquals(42, a.getInt("count", -1));
        assertEquals(0, a.getInt("zero", -1));
        assertEquals(-7, a.getInt("negative", -1));
        assertEquals(99, a.getInt("missing", 99));
    }

    private static JsonValue json(String src) {
        try {
            return JsonParser.parse(src);
        }
        catch (JsonParseException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> yaml(String src) {
        Map<String, Object> m = new Yaml().load(src);
        return m == null ? new HashMap<>() : m;
    }
}
