package io.synadia.flink.v0.source.reader;

import java.io.IOException;

@FunctionalInterface
public interface Callback<T, R> {
    R newConnection(T input) throws IOException;
}
