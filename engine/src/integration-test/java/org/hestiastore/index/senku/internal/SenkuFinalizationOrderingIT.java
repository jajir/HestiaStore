package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.SenkuFinalizationTestRuntime.BLOCK_BYTES;
import static org.hestiastore.index.senku.internal.SenkuFinalizationTestRuntime.KEYS;
import static org.hestiastore.index.senku.internal.SenkuFinalizationTestRuntime.VALUES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.senku.SenkuIndex;
import org.hestiastore.index.senku.SenkuReady;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Exercises exact finalization task orderings with real filesystem merges. */
class SenkuFinalizationOrderingIT {

    @TempDir
    private Path temporaryDirectory;

    @ParameterizedTest(name = "tail={0}, scanFirst={1}, shards={2}")
    @CsvSource({ "false,false,1", "false,false,3", "true,false,1",
            "true,false,3", "false,true,1", "false,true,3", "true,true,1",
            "true,true,3" })
    void finalizationDiscoversEveryFlushBeforePublishingReady(
            final boolean tail, final boolean scanFirst, final int shards)
            throws Exception {
        final int threshold = tail ? 2 : 1;
        try (var runtime = SenkuFinalizationTestRuntime.createStarted(
                new FsDirectory(temporaryDirectory.toFile()), shards, threshold,
                (key, left, right) -> left + right)) {
            publishInitialFlushes(runtime, tail);
            runtime.control.runPeriodicScan();
            assertEquals(2, runtime.coordinator.flushCount());
            assertEquals(shards, runtime.coordinator.submittedCount());
            final List<Runnable> oldCompletions = scanFirst ? List.of()
                    : runtime.finishReservedJobs();

            final int newKey = tail ? 3 : 2;
            runtime.writing.put(newKey, 50L);
            assertEquals(tail ? 2 : 3, runtime.flushNames().size(),
                    "Threshold one publishes the new flush before finishWriting.");
            final Runnable finishingScan = runtime.startFinish();
            assertEquals(SenkuWritingState.FINISHING, runtime.writing.state());
            assertEquals(3, runtime.flushNames().size());

            if (scanFirst) {
                finishingScan.run();
                assertEquals(3, runtime.coordinator.flushCount());
                runtime.finishReservedJobs().forEach(Runnable::run);
                assertFalse(runtime.coordinator.isDrainComplete());
            } else {
                oldCompletions.forEach(Runnable::run);
                assertEquals(0, runtime.coordinator.flushCount());
                assertEquals(List.of(SenkuFileNames.flushDirectory(2)),
                        runtime.flushNames());
                assertEquals(List.of(Entry.of(newKey, 50L)),
                        runtime.readFlush(2));
                assertFalse(
                        runtime.root.isFileExists(SenkuFileNames.READY_FILE),
                        "An old completion must not publish ready before the final scan.");
                assertTrue(runtime.coordinator.isDrainComplete(),
                        "This schedule deliberately makes the cached catalog stale.");
                assertEquals(SenkuWritingState.FINISHING,
                        runtime.writing.state());
                assertNull(runtime.firstFailure.get());
                finishingScan.run();
            }

            runtime.drain();
            final List<Entry<Integer, Long>> expected = tail
                    ? List.of(Entry.of(1, 40L), Entry.of(2, 60L),
                            Entry.of(3, 50L))
                    : List.of(Entry.of(1, 30L), Entry.of(2, 50L));
            assertReadyAndReopen(runtime, expected);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 3 })
    void remainingMergeFailureNeverPublishesReady(final int shards)
            throws Exception {
        final IndexException expectedFailure = new IndexException(
                "Injected failure while merging the newly discovered flushes.");
        try (var runtime = SenkuFinalizationTestRuntime.createStarted(
                new FsDirectory(temporaryDirectory.toFile()), shards, 1,
                (key, left, right) -> {
                    if (key == 2) {
                        throw expectedFailure;
                    }
                    return left + right;
                })) {
            publishInitialFlushes(runtime, false);
            runtime.control.runPeriodicScan();
            final List<Runnable> oldCompletions = runtime.finishReservedJobs();
            runtime.writing.put(2, 40L);
            runtime.writing.put(2, 50L);
            final Runnable finishingScan = runtime.startFinish();

            oldCompletions.forEach(Runnable::run);
            assertFalse(runtime.root.isFileExists(SenkuFileNames.READY_FILE));
            assertEquals(List.of(Entry.of(2, 40L)), runtime.readFlush(2));
            assertEquals(List.of(Entry.of(2, 50L)), runtime.readFlush(3));
            finishingScan.run();
            runtime.drain();

            final ExecutionException actual = assertThrows(
                    ExecutionException.class, runtime::awaitReady);
            assertSame(expectedFailure, actual.getCause());
            assertSame(expectedFailure, runtime.firstFailure.get());
            assertFalse(runtime.root.isFileExists(SenkuFileNames.READY_FILE));
            assertFalse(runtime.root.isFileExists(SenkuFileNames.LOCK_FILE));
            assertTrue(runtime.control.isTerminated());
            assertTrue(runtime.workers.isTerminated());
            assertThrows(IndexException.class, () -> SenkuIndex
                    .open(runtime.root, KEYS, VALUES, BLOCK_BYTES));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 3 })
    void emptyFinishCreatesReadableEmptyTerminalRuns(final int shards)
            throws Exception {
        try (var runtime = SenkuFinalizationTestRuntime.createStarted(
                new FsDirectory(temporaryDirectory.toFile()), shards, 1,
                (key, left, right) -> left + right)) {
            runtime.startFinish().run();
            runtime.drain();
            assertReadyAndReopen(runtime, List.of());
        }
    }

    private static void publishInitialFlushes(
            final SenkuFinalizationTestRuntime runtime, final boolean tail) {
        runtime.writing.put(1, 10L);
        if (tail) {
            runtime.writing.put(2, 20L);
            runtime.writing.put(1, 30L);
            runtime.writing.put(2, 40L);
        } else {
            runtime.writing.put(1, 20L);
        }
        assertEquals(2, runtime.flushNames().size());
    }

    private static void assertReadyAndReopen(
            final SenkuFinalizationTestRuntime runtime,
            final List<Entry<Integer, Long>> expected) throws Exception {
        try (SenkuReady<Integer, Long> ready = runtime.awaitReady();
                var stream = ready.openStream()) {
            assertEquals(expected, stream.toList());
            assertTrue(runtime.root.isFileExists(SenkuFileNames.READY_FILE));
            assertTrue(runtime.root.isFileExists(SenkuFileNames.LOCK_FILE));
            assertEquals(List.of(), runtime.flushNames());
            assertTrue(runtime.control.isTerminated());
            assertTrue(runtime.workers.isTerminated());
        }
        assertFalse(runtime.root.isFileExists(SenkuFileNames.LOCK_FILE));
        try (var reopened = SenkuIndex.open(runtime.root, KEYS, VALUES,
                BLOCK_BYTES); var stream = reopened.openStream()) {
            assertEquals(expected, stream.toList());
        }
        assertFalse(runtime.root.isFileExists(SenkuFileNames.LOCK_FILE));
    }
}
