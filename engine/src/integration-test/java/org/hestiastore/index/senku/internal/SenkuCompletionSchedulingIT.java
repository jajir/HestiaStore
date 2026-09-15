package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.Entry;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

/** Deterministic completion scheduling and strict finalization validation. */
@ExtendWith(MockitoExtension.class)
class SenkuCompletionSchedulingIT {

    @Spy
    private MemDirectory directory;

    @Test
    void writingCompletionsStartAlreadyDiscoveredWorkWithoutAnotherScan()
            throws Exception {
        try (var runtime = createRuntime(3)) {
            publishFirstGroup(runtime);
            runtime.control.runPeriodicScan();
            runtime.writing.put(2, 40L);
            runtime.writing.put(2, 50L);
            runtime.control.runPeriodicScan();
            assertEquals(4, runtime.coordinator.flushCount());
            runtime.finishReservedJobs().forEach(Runnable::run);
            assertEquals(2, runtime.coordinator.flushCount());
            assertFalse(runtime.workers.hasPendingTasks());

            runtime.control.takeTask().run();

            assertTrue(runtime.workers.hasPendingTasks());
            assertEquals(3, runtime.coordinator.submittedCount());
            assertEquals(SenkuWritingState.WRITING, runtime.writing.state());
            assertFalse(directory.isFileExists(SenkuFileNames.READY_FILE));
            runtime.startFinish().run();
            runtime.drain();
            try (var ready = runtime.awaitReady();
                    var stream = ready.openStream()) {
                assertEquals(List.of(Entry.of(1, 30L), Entry.of(2, 90L)),
                        stream.toList());
            }
        }
    }

    @Test
    void firstFinishingReconciliationRejectsChangedCachedManifest()
            throws Exception {
        try (var runtime = createRuntime(1)) {
            publishFirstGroup(runtime);
            runtime.control.runPeriodicScan();
            runtime.finishReservedJobs().forEach(Runnable::run);
            runtime.control.takeTask().run();
            runtime.writing.put(2, 40L);
            runtime.writing.put(2, 50L);
            runtime.control.runPeriodicScan();
            assertTrue(runtime.workers.hasPendingTasks());
            assertFalse(runtime.coordinator.isDrainComplete(),
                    "Only the first strict drain scan can detect corruption now.");
            final Directory run = terminalRun(0);
            run.deleteFile(SenkuFileNames.MANIFEST_FILE);
            SenkuMetadataCodec.publishRunManifest(run,
                    new SenkuRunManifest(1, 2));

            runtime.startFinish().run();

            assertThrows(ExecutionException.class, runtime::awaitReady);
            assertNotNull(runtime.firstFailure.get());
            assertFalse(directory.isFileExists(SenkuFileNames.READY_FILE));
            assertFalse(directory.isFileExists(SenkuFileNames.LOCK_FILE));
        }
    }

    @Test
    void finalReadinessCheckRevalidatesManifestsPublishedDuringDrain()
            throws Exception {
        try (var runtime = createRuntime(3)) {
            runtime.startFinish().run();
            runtime.workers.runNextTask();
            runtime.control.takeTask().run();
            // Hold the coalesced wake while remaining completions arrive.
            final Runnable completionWake = runtime.control.takeTask();
            runtime.finishReservedJobs().forEach(Runnable::run);
            assertTrue(runtime.coordinator.isDrainComplete());
            final Directory run = terminalRun(0);
            run.deleteFile(SenkuFileNames.MANIFEST_FILE);
            SenkuMetadataCodec.publishRunManifest(run,
                    new SenkuRunManifest(1, 1));

            completionWake.run();

            assertThrows(ExecutionException.class, runtime::awaitReady);
            assertNotNull(runtime.firstFailure.get());
            assertFalse(directory.isFileExists(SenkuFileNames.READY_FILE));
            assertFalse(directory.isFileExists(SenkuFileNames.LOCK_FILE));
        }
    }

    @Test
    void writingTickCannotPublishReadyWhenFinishStartsDuringDiscovery()
            throws Exception {
        try (var runtime = createRuntime(1)) {
            publishFirstGroup(runtime);
            runtime.control.runPeriodicScan();
            runtime.finishReservedJobs().forEach(Runnable::run);
            runtime.control.takeTask().run();
            assertTrue(runtime.coordinator.isDrainComplete());
            final AtomicBoolean startFinish = new AtomicBoolean(true);
            final AtomicReference<Runnable> finishingScan = new AtomicReference<>();
            doAnswer(invocation -> {
                if (startFinish.getAndSet(false)) {
                    finishingScan.set(runtime.startFinish());
                }
                return invocation.callRealMethod();
            }).when(directory).getFileNames();

            runtime.control.runPeriodicScan();

            assertEquals(SenkuWritingState.FINISHING, runtime.writing.state());
            assertNotNull(finishingScan.get());
            assertFalse(directory.isFileExists(SenkuFileNames.READY_FILE));
            finishingScan.get().run();
            try (var ready = runtime.awaitReady();
                    var stream = ready.openStream()) {
                assertEquals(List.of(Entry.of(1, 30L)), stream.toList());
            }
        }
    }

    private SenkuFinalizationTestRuntime createRuntime(final int shards) {
        return SenkuFinalizationTestRuntime.createStarted(directory, shards, 1,
                (key, left, right) -> left + right);
    }

    private static void publishFirstGroup(
            final SenkuFinalizationTestRuntime runtime) {
        runtime.writing.put(1, 10L);
        runtime.writing.put(1, 20L);
    }

    private Directory terminalRun(final int shard) {
        return directory.openSubDirectory(SenkuFileNames.shardDirectory(shard))
                .openSubDirectory(SenkuFileNames.levelDirectory(0))
                .openSubDirectory(SenkuFileNames.runDirectory(0));
    }
}
