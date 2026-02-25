package io.synadia.flink.source;

import io.synadia.flink.TestBase;
import io.synadia.flink.enumerator.NatsSourceEnumerator;
import io.synadia.flink.source.split.NatsSubjectSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests that the enumerator correctly reassigns splits after a reader failure.
 * Covers the fix for addSplitsBack -> handleSplitRequest flow.
 */
class NatsSourceEnumeratorRestartTests extends TestBase {

    @SuppressWarnings("unchecked")
    private NatsSourceEnumerator<NatsSubjectSplit> createEnumerator(
        SplitEnumeratorContext<NatsSubjectSplit> context, List<String> subjects)
    {
        Queue<NatsSubjectSplit> splits = subjects.stream()
            .map(NatsSubjectSplit::new)
            .collect(Collectors.toCollection(ArrayDeque::new));
        return new NatsSourceEnumerator<>(context, splits);
    }

    @Test
    @DisplayName("Single reader restart: splits are reassigned after addSplitsBack")
    @SuppressWarnings("unchecked")
    void testSingleReaderRestart() {
        SplitEnumeratorContext<NatsSubjectSplit> context = mock(SplitEnumeratorContext.class);
        when(context.currentParallelism()).thenReturn(1);

        List<String> subjects = List.of("subject.a", "subject.b");
        NatsSourceEnumerator<NatsSubjectSplit> enumerator = createEnumerator(context, subjects);
        enumerator.start();

        // Initial assignment: reader 0 gets all splits
        enumerator.addReader(0);

        ArgumentCaptor<SplitsAssignment<NatsSubjectSplit>> captor =
            ArgumentCaptor.forClass(SplitsAssignment.class);
        verify(context, times(1)).assignSplits(captor.capture());

        List<NatsSubjectSplit> initialSplits = captor.getValue().assignment().get(0);
        assertEquals(2, initialSplits.size());

        // Simulate reader 0 failure: Flink returns the splits
        enumerator.addSplitsBack(initialSplits, 0);

        // Reader 0 restarts and re-registers
        enumerator.addReader(0);

        // Should have been called twice total (initial + after restart)
        verify(context, times(2)).assignSplits(captor.capture());
        verify(context, never()).signalNoMoreSplits(anyInt());

        List<NatsSubjectSplit> reassignedSplits = captor.getValue().assignment().get(0);
        assertEquals(2, reassignedSplits.size(), "All splits should be reassigned after restart");
    }

    @Test
    @DisplayName("Only failed reader gets reassigned, healthy reader is untouched")
    @SuppressWarnings("unchecked")
    void testPartialReaderFailure() {
        SplitEnumeratorContext<NatsSubjectSplit> context = mock(SplitEnumeratorContext.class);
        when(context.currentParallelism()).thenReturn(2);

        List<String> subjects = List.of("subject.a", "subject.b");
        NatsSourceEnumerator<NatsSubjectSplit> enumerator = createEnumerator(context, subjects);
        enumerator.start();

        // Initial assignment: reader 0 and reader 1 each get 1 split
        enumerator.addReader(0);
        enumerator.addReader(1);

        ArgumentCaptor<SplitsAssignment<NatsSubjectSplit>> captor =
            ArgumentCaptor.forClass(SplitsAssignment.class);
        verify(context, times(2)).assignSplits(captor.capture());

        // Capture what reader 1 got
        List<NatsSubjectSplit> reader1Splits = captor.getAllValues().get(1).assignment().get(1);
        assertEquals(1, reader1Splits.size());

        // Reader 1 fails, its splits come back
        enumerator.addSplitsBack(reader1Splits, 1);

        // Reader 1 restarts
        enumerator.addReader(1);

        // 3 total assignments: initial reader 0, initial reader 1, restart reader 1
        verify(context, times(3)).assignSplits(captor.capture());

        List<NatsSubjectSplit> reassigned = captor.getValue().assignment().get(1);
        assertEquals(1, reassigned.size(), "Failed reader should get its split back");
        assertEquals(reader1Splits.get(0).getSubject(), reassigned.get(0).getSubject());
    }

    @Test
    @DisplayName("Multiple restarts: splits survive repeated failures")
    @SuppressWarnings("unchecked")
    void testMultipleRestarts() {
        SplitEnumeratorContext<NatsSubjectSplit> context = mock(SplitEnumeratorContext.class);
        when(context.currentParallelism()).thenReturn(1);

        List<String> subjects = List.of("subject.a");
        NatsSourceEnumerator<NatsSubjectSplit> enumerator = createEnumerator(context, subjects);
        enumerator.start();

        ArgumentCaptor<SplitsAssignment<NatsSubjectSplit>> captor =
            ArgumentCaptor.forClass(SplitsAssignment.class);

        for (int restart = 0; restart < 5; restart++) {
            enumerator.addReader(0);
            verify(context, times(restart + 1)).assignSplits(captor.capture());

            List<NatsSubjectSplit> splits = captor.getValue().assignment().get(0);
            assertEquals(1, splits.size(), "Restart " + restart + ": should get the split");

            // Simulate failure and return splits
            enumerator.addSplitsBack(splits, 0);
        }

        verify(context, never()).signalNoMoreSplits(anyInt());
    }

    @Test
    @DisplayName("snapshotState captures splits that haven't been assigned yet")
    @SuppressWarnings("unchecked")
    void testSnapshotIncludesUnassignedPrecomputedSplits() throws Exception {
        SplitEnumeratorContext<NatsSubjectSplit> context = mock(SplitEnumeratorContext.class);
        when(context.currentParallelism()).thenReturn(3);

        List<String> subjects = List.of("subject.a", "subject.b", "subject.c");
        NatsSourceEnumerator<NatsSubjectSplit> enumerator = createEnumerator(context, subjects);
        enumerator.start();

        // Only reader 0 registers — readers 1 and 2 haven't come up yet
        enumerator.addReader(0);

        // Snapshot should contain the 2 unassigned splits (for readers 1 and 2)
        Collection<NatsSubjectSplit> snapshot = enumerator.snapshotState(1L);
        Set<String> snapshotSubjects = snapshot.stream()
            .map(NatsSubjectSplit::getSubject)
            .collect(Collectors.toSet());

        assertEquals(2, snapshot.size(), "Snapshot must capture splits not yet assigned");
        assertTrue(snapshotSubjects.contains("subject.b") || snapshotSubjects.contains("subject.c"),
            "Snapshot should contain pending splits");
    }

    @Test
    @DisplayName("snapshotState captures splits returned via addSplitsBack")
    @SuppressWarnings("unchecked")
    void testSnapshotIncludesAddedBackSplits() throws Exception {
        SplitEnumeratorContext<NatsSubjectSplit> context = mock(SplitEnumeratorContext.class);
        when(context.currentParallelism()).thenReturn(1);

        List<String> subjects = List.of("subject.a");
        NatsSourceEnumerator<NatsSubjectSplit> enumerator = createEnumerator(context, subjects);
        enumerator.start();

        // Reader 0 gets its split
        enumerator.addReader(0);
        ArgumentCaptor<SplitsAssignment<NatsSubjectSplit>> captor =
            ArgumentCaptor.forClass(SplitsAssignment.class);
        verify(context, times(1)).assignSplits(captor.capture());
        List<NatsSubjectSplit> assigned = captor.getValue().assignment().get(0);

        // Reader 0 fails — splits come back
        enumerator.addSplitsBack(assigned, 0);

        // Snapshot should now contain the returned split
        Collection<NatsSubjectSplit> snapshot = enumerator.snapshotState(1L);
        assertEquals(1, snapshot.size(), "Snapshot must include splits returned via addSplitsBack");
        assertEquals("subject.a", snapshot.iterator().next().getSubject());
    }

    @Test
    @DisplayName("snapshotState is empty after all splits are assigned")
    @SuppressWarnings("unchecked")
    void testSnapshotEmptyAfterFullAssignment() throws Exception {
        SplitEnumeratorContext<NatsSubjectSplit> context = mock(SplitEnumeratorContext.class);
        when(context.currentParallelism()).thenReturn(2);

        List<String> subjects = List.of("subject.a", "subject.b");
        NatsSourceEnumerator<NatsSubjectSplit> enumerator = createEnumerator(context, subjects);
        enumerator.start();

        // Both readers register — all splits assigned
        enumerator.addReader(0);
        enumerator.addReader(1);

        Collection<NatsSubjectSplit> snapshot = enumerator.snapshotState(1L);
        assertTrue(snapshot.isEmpty(),
            "Snapshot should be empty when all splits have been assigned to readers");
    }

    @Test
    @DisplayName("Restored enumerator from snapshot: all readers come back after JM failover")
    @SuppressWarnings("unchecked")
    void testRestoreFromSnapshot() throws Exception {
        SplitEnumeratorContext<NatsSubjectSplit> context = mock(SplitEnumeratorContext.class);
        when(context.currentParallelism()).thenReturn(2);

        List<String> subjects = List.of("subject.a", "subject.b");
        NatsSourceEnumerator<NatsSubjectSplit> enumerator = createEnumerator(context, subjects);
        enumerator.start();

        // Only reader 0 registers before checkpoint; reader 1 hasn't come up yet
        enumerator.addReader(0);

        // Snapshot captures the 1 unassigned split (meant for reader 1)
        Collection<NatsSubjectSplit> snapshot = enumerator.snapshotState(1L);
        assertEquals(1, snapshot.size());

        // JM failover: ALL readers die. New enumerator restored from snapshot.
        // The snapshot only has the 1 split that was never assigned.
        // Reader 0's split is restored via reader 0's own checkpoint (not the enumerator).
        SplitEnumeratorContext<NatsSubjectSplit> context2 = mock(SplitEnumeratorContext.class);
        when(context2.currentParallelism()).thenReturn(2);

        NatsSourceEnumerator<NatsSubjectSplit> restored =
            new NatsSourceEnumerator<>(context2, snapshot);
        restored.start();

        // Both readers come back after JM failover
        restored.addReader(0);
        restored.addReader(1);

        ArgumentCaptor<SplitsAssignment<NatsSubjectSplit>> captor =
            ArgumentCaptor.forClass(SplitsAssignment.class);

        // Only 1 split in the snapshot → only 1 reader gets an assignment
        verify(context2, times(1)).assignSplits(captor.capture());
        // The other reader gets signalNoMoreSplits
        verify(context2, times(1)).signalNoMoreSplits(anyInt());

        // The one assignment should contain exactly the 1 surviving split
        SplitsAssignment<NatsSubjectSplit> assignment = captor.getValue();
        List<NatsSubjectSplit> assignedSplits = assignment.assignment().values().iterator().next();
        assertEquals(1, assignedSplits.size(),
            "Only the unassigned split from snapshot should be distributed");
    }

    @Test
    @DisplayName("Without fix: empty addSplitsBack does not cause spurious assignment")
    @SuppressWarnings("unchecked")
    void testEmptyAddSplitsBack() {
        SplitEnumeratorContext<NatsSubjectSplit> context = mock(SplitEnumeratorContext.class);
        when(context.currentParallelism()).thenReturn(1);

        List<String> subjects = List.of("subject.a");
        NatsSourceEnumerator<NatsSubjectSplit> enumerator = createEnumerator(context, subjects);
        enumerator.start();

        enumerator.addReader(0);
        verify(context, times(1)).assignSplits(any());

        // Empty splits back (edge case)
        enumerator.addSplitsBack(List.of(), 0);

        // Reader re-registers
        enumerator.addReader(0);

        // No new assignment should happen, should signal no more splits
        verify(context, times(1)).assignSplits(any());
        verify(context, times(1)).signalNoMoreSplits(0);
    }
}
