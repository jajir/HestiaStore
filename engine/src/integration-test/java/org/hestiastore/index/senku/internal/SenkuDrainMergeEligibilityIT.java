package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Exercises partial-drain admission with actual, manually released L0 jobs. */
class SenkuDrainMergeEligibilityIT {

    private static final DataBlockSize BLOCK = DataBlockSize
            .ofDataBlockSize(1024);

    private MemDirectory root;
    private Directory flush;
    private SenkuManualControlExecutor control;
    private SenkuManualWorkerExecutor workers;
    private AtomicReference<IndexException> failure;
    private SenkuMaintenanceCoordinator<Integer, Long> coordinator;

    @BeforeEach
    void setUp() {
        root = new MemDirectory();
        root.mkdir(SenkuFileNames.FLUSH_DIRECTORY);
        flush = root.openSubDirectory(SenkuFileNames.FLUSH_DIRECTORY);
        control = new SenkuManualControlExecutor();
        workers = new SenkuManualWorkerExecutor();
        failure = new AtomicReference<>();
        coordinator = new SenkuMaintenanceCoordinator<>(root, 2, 4,
                new TypeDescriptorInteger(), new TypeDescriptorLong(),
                (key, left, right) -> left + right, 1, 4L, BLOCK, workers,
                control, failure, ignored -> {
                    // This test observes scheduling, not ingestion admission.
                });
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        workers.shutdownNow();
        control.shutdownNow();
        assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        assertTrue(control.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void partialRunMergeWaitsForTheLastL0ShardAndIncludesItsOutput()
            throws InterruptedException {
        writeRun(0);
        writeRun(1);
        writeFlush(0);
        writeFlush(1);

        coordinator.scanAndScheduleOnce(true);
        assertTrue(coordinator.hasActiveL0Batch());
        assertEquals(2, coordinator.submittedCount());
        assertFalse(hasLevelOne());

        completeNextJob();
        coordinator.scanAndScheduleOnce(true);
        assertTrue(coordinator.hasActiveL0Batch());
        assertEquals(1, coordinator.submittedCount());
        assertFalse(hasLevelOne());

        completeNextJob();
        assertFalse(coordinator.hasActiveL0Batch());
        assertEquals(0, coordinator.flushCount());
        coordinator.scanAndScheduleOnce(true);
        assertEquals(1, coordinator.submittedCount());
        assertTrue(hasLevelOne());

        completeNextJob();
        coordinator.scanAndScheduleOnce(true);
        assertTrue(coordinator.isDrainComplete());
        assertEquals(2, coordinator.runCount());
        final Directory level = root
                .openSubDirectory(SenkuFileNames.shardDirectory(0))
                .openSubDirectory(SenkuFileNames.levelDirectory(1));
        final Directory run = level
                .openSubDirectory(SenkuFileNames.runDirectory(0));
        final SenkuRunManifest manifest = SenkuMetadataCodec
                .readRunManifest(run);
        assertEquals(1, manifest.recordCount());
        final SenkuMergeSource source = SenkuMergeSource.run(
                new LargeFile(run, BLOCK, 4L, manifest.partCount()),
                manifest.recordCount());
        try (var entries = source.open(new TypeDescriptorInteger(),
                new TypeDescriptorLong())) {
            assertEquals(Entry.of(0, 4L), entries.next());
            assertFalse(entries.hasNext());
        }
        assertNull(failure.get());
    }

    @Test
    void fullFanInRunMergeRemainsEligibleWhileL0WorkIsOutstanding() {
        for (long run = 0; run < 4; run++) {
            writeRun(run);
        }
        writeFlush(0);

        coordinator.scanAndScheduleOnce(true);

        assertTrue(coordinator.hasActiveL0Batch());
        assertEquals(3, coordinator.submittedCount());
        assertEquals(3, workers.getQueue().size());
        assertTrue(hasLevelOne());
        assertNull(failure.get());
    }

    private void completeNextJob() throws InterruptedException {
        workers.runNextTask();
        control.takeTask().run();
        assertNull(failure.get());
    }

    private boolean hasLevelOne() {
        return root.openSubDirectory(SenkuFileNames.shardDirectory(0))
                .isFileExists(SenkuFileNames.levelDirectory(1));
    }

    private void writeFlush(final long generation) {
        new SenkuFlushWriter<>(flush, new TypeDescriptorInteger(),
                new TypeDescriptorLong(), key -> key, 2, 1, 4L, BLOCK)
                .write(generation, List.of(Map.of(0, 1L, 1, 1L)));
    }

    private void writeRun(final long runId) {
        final String shardName = SenkuFileNames.shardDirectory(0);
        if (!root.isFileExists(shardName)) {
            root.mkdir(shardName);
        }
        final Directory shard = root.openSubDirectory(shardName);
        final String levelName = SenkuFileNames.levelDirectory(0);
        if (!shard.isFileExists(levelName)) {
            shard.mkdir(levelName);
        }
        final Directory level = shard.openSubDirectory(levelName);
        final String runName = SenkuFileNames.runDirectory(runId);
        level.mkdir(runName);
        new SenkuRunWriter<>(level.openSubDirectory(runName),
                new TypeDescriptorInteger(), new TypeDescriptorLong(), 1, 4L,
                BLOCK).write(new EntryIteratorList<>(List.of(Entry.of(0, 1L))));
    }
}
