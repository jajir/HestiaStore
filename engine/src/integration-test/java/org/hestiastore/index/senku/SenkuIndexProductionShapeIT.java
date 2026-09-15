package org.hestiastore.index.senku;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FsDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SenkuIndexProductionShapeIT {

    private static final int SHARD_COUNT = 128;
    private static final int CALLER_COUNT = 8;
    private static final int ENTRIES_PER_CALLER = 65;
    private static final int ENTRY_COUNT = CALLER_COUNT * ENTRIES_PER_CALLER;
    private static final int CYCLE_COUNT = 2;

    @TempDir
    private File tempDirectory;

    @Test
    void filesystemFinalizationDrainsEveryShardAcrossRepeatedCycles()
            throws Exception {
        final ExecutorService callers = Executors
                .newFixedThreadPool(CALLER_COUNT);
        try {
            for (int cycle = 0; cycle < CYCLE_COUNT; cycle++) {
                runCycle(callers, cycle);
            }
        } finally {
            callers.shutdownNow();
            assertTrue(callers.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    private void runCycle(final ExecutorService callers, final int cycle)
            throws Exception {
        final Directory directory = new FsDirectory(tempDirectory)
                .openSubDirectory("cycle-" + cycle);
        final SenkuWriting<Long, NullValue> writing = createWriting(directory);
        putConcurrently(callers, writing);

        final SenkuReady<Long, NullValue> completed = writing.finishWriting();
        assertKeys(completed);
        completed.close();

        final SenkuReady<Long, NullValue> reopened = SenkuIndex.open(directory,
                new TypeDescriptorLong(), new TypeDescriptorNull(), 8_192);
        assertKeys(reopened);
        reopened.close();
    }

    private static SenkuWriting<Long, NullValue> createWriting(
            final Directory directory) {
        final SenkuMergeFunctionRegistry<Long, NullValue> functions =
                new SenkuMergeFunctionRegistry<>();
        functions.register((key, first, second) -> first);
        return SenkuIndex
                .builder(directory, new TypeDescriptorLong(),
                        new TypeDescriptorNull(), functions)
                .shardHashFunction(key -> key.hashCode())
                .shardCount(SHARD_COUNT)
                .maxInMemoryEntries(ENTRIES_PER_CALLER)
                .maxKeysPerPage(1_000_000).mergeFanIn(64)
                .maintenanceThreads(8).maintenanceQueueSize(120)
                .diskIoBufferSize(8_192)
                .maxEntriesPerPart(10_000_000L).create();
    }

    private static void putConcurrently(final ExecutorService callers,
            final SenkuWriting<Long, NullValue> writing) throws Exception {
        final CountDownLatch start = new CountDownLatch(1);
        final List<Future<?>> writes = new ArrayList<>(CALLER_COUNT);
        for (int caller = 0; caller < CALLER_COUNT; caller++) {
            final long firstKey = (long) caller * ENTRIES_PER_CALLER;
            writes.add(callers.submit(() -> {
                start.await();
                for (long offset = 0; offset < ENTRIES_PER_CALLER; offset++) {
                    writing.put(firstKey + offset, NULL);
                }
                return null;
            }));
        }
        start.countDown();
        for (final Future<?> write : writes) {
            write.get(30, TimeUnit.SECONDS);
        }
    }

    private static void assertKeys(final SenkuReady<Long, NullValue> ready) {
        final List<Entry<Long, NullValue>> expected = LongStream
                .range(0, ENTRY_COUNT).mapToObj(key -> Entry.of(key, NULL))
                .toList();
        try (Stream<Entry<Long, NullValue>> entries = ready.openStream()) {
            assertEquals(expected, entries.toList());
        }
    }
}
