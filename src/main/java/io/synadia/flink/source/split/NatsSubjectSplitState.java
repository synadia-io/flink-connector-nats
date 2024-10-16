package io.synadia.flink.source.split;

public class NatsSubjectSplitState {

    private NatsSubjectSplit split;

    public NatsSubjectSplitState(NatsSubjectSplit split) {
        this.split = split;
    }

    public NatsSubjectSplit toNatsSubjectSplit() {
        return new NatsSubjectSplit(split.getSubject(),split.getCurrentMessages());
    }

    public NatsSubjectSplit getSplit() {
        return split;
    }
}
