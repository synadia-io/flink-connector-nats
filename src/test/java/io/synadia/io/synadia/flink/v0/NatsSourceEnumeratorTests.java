package io.synadia.io.synadia.flink.v0;

import io.synadia.flink.v0.enumerator.NatsSourceEnumerator;
import io.synadia.flink.v0.source.split.NatsSubjectSplit;
import io.synadia.io.synadia.flink.TestBase;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class NatsSourceEnumeratorTests extends TestBase {

    private SplitEnumeratorContext<NatsSubjectSplit> context;
    private Queue<NatsSubjectSplit> splitsQueue;
    private NatsSourceEnumerator enumerator;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        context = mock(SplitEnumeratorContext.class);
        splitsQueue = new ArrayDeque<>();
    }

    static Stream<TestParameters> provideTestParameters() {
        return Stream.of(
                new TestParameters(5, 3, generateSplits(3), "Splits < Parallelism"),
                new TestParameters(3, 5, generateSplits(5), "Splits > Parallelism"),
                new TestParameters(3, 3, generateSplits(3), "Splits == Parallelism"),
                new TestParameters(89, 9, generateSplits(9), "High Parallelism with few splits, Splits <<<< Parallelism"),
                new TestParameters(5, 100, generateSplits(100), "More Splits with less parallelism, Splits >>>>>> Parallelism")
        );
    }

    @DisplayName("NatsSourceEnumerator Parameterized Test")
    @ParameterizedTest(name = "{index} - {0}")
    @MethodSource("provideTestParameters")
    void testHandleSplitRequest(TestParameters params) {
        // Setup splits queue
        splitsQueue.addAll(params.splits.stream().map(NatsSubjectSplit::new).collect(Collectors.toList()));

        when(context.currentParallelism()).thenReturn(params.parallelism);
        enumerator = new NatsSourceEnumerator("test", context, splitsQueue);

        // precompute split assignments
        enumerator.start();

        for (int subtaskId = 0; subtaskId < params.parallelism; subtaskId++) {
            enumerator.handleSplitRequest(subtaskId, null);
        }

        // Capture arguments passed to assignSplits
        ArgumentCaptor<SplitsAssignment<NatsSubjectSplit>> captor = ArgumentCaptor.forClass(SplitsAssignment.class);
        verify(context, times(params.expectedInvocations)).assignSplits(captor.capture());

        List<SplitsAssignment<NatsSubjectSplit>> assignments = captor.getAllValues();
        assertEquals(params.expectedInvocations, assignments.size());

        // Verify splits assigned
        List<String> assignedSplits = new ArrayList<>();
        for (SplitsAssignment<NatsSubjectSplit> assignment : assignments) {
            assignment.assignment().values().forEach(splits -> {
                assertTrue(splits.size() <= params.splits.size() / params.parallelism + 1, "Each subtask should get the correct number of splits");
                splits.forEach(split -> assignedSplits.add(split.getSubject()));
            });
        }

        assertEquals(params.splits, assignedSplits);
    }

    static List<String> generateSplits(int count) {
        List<String> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add("split" + i);
        }
        return splits;
    }

    static class TestParameters {
        final int parallelism;
        final int splitsCount;
        final List<String> splits;
        final int expectedInvocations;
        final String description;

        TestParameters(int parallelism, int splitsCount, List<String> splits, String description) {
            this.parallelism = parallelism;
            this.splitsCount = splitsCount;
            this.splits = splits;
            this.expectedInvocations = Math.min(parallelism, splitsCount);
            this.description = description;
        }

        @Override
        public String toString() {
            return description;
        }
    }
}