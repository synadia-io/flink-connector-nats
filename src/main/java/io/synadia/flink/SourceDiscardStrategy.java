// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink;

public enum SourceDiscardStrategy {
    New("new"),
    Old("old"),
    Fail("fail");

    private final String text;

    SourceDiscardStrategy(String p) {
        text = p;
    }

    @Override
    public String toString() {
        return text;
    }

    public static SourceDiscardStrategy get(String value) {
        String lower = value.toLowerCase();
        for (SourceDiscardStrategy sds : SourceDiscardStrategy.values()) {
            if (sds.text.equals(lower)) {
                return sds;
            }
        }
        return null;
    }
}
