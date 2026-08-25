package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuL0BatchTest {

    private static final int SHARD_COUNT = 2;

    private TypeDescriptorInteger keys;
    private TypeDescriptorLong values;
    private MemDirectory flushDirectory;

    @BeforeEach
    void setUp() {
        keys = new TypeDescriptorInteger();
        values = new TypeDescriptorLong();
        flushDirectory = new MemDirectory();
        writeFlush(0L, entries(0, 1L, 1, 2L));
        writeFlush(1L, entries(0, 4L, 3, 8L));
    }

    @Test
    void loadsSelectedFlushesAndCreatesIndependentShardSources() {
        final SenkuL0Batch batch = load(new long[] { 0L, 1L },
                new long[] { 5L, 6L });

        assertArrayEquals(new long[] { 0L, 1L }, batch.inputFlushIds());
        assertEquals(2, batch.sourcesForShard(0).size());
        assertEquals(2, batch.sourcesForShard(1).size());
        assertEquals(5L, batch.outputRunId(0));
        assertEquals(6L, batch.outputRunId(1));
    }

    @Test
    void acceptsSubmittedShardJobsInAnyCompletionOrderWithoutDeletingInputs() {
        final SenkuL0Batch batch = load(new long[] { 0L, 1L },
                new long[] { 5L, 6L });
        assertEquals(0, batch.takeNextShard().orElseThrow());
        assertEquals(1, batch.takeNextShard().orElseThrow());
        assertTrue(batch.takeNextShard().isEmpty());

        batch.accept(new SenkuCompletedRun(1, 0, 6L,
                new SenkuRunManifest(1, 2L)));
        assertFalse(batch.isComplete());
        batch.accept(new SenkuCompletedRun(0, 0, 5L,
                new SenkuRunManifest(1, 1L)));

        assertTrue(batch.isComplete());
        assertTrue(flushDirectory
                .isFileExists(SenkuFileNames.flushDirectory(0L)));
        assertTrue(flushDirectory
                .isFileExists(SenkuFileNames.flushDirectory(1L)));
    }

    @Test
    void supportsDrainModePartialSelection() {
        final SenkuL0Batch batch = load(new long[] { 1L },
                new long[] { 0L, 0L });

        assertArrayEquals(new long[] { 1L }, batch.inputFlushIds());
        assertEquals(1, batch.sourcesForShard(0).size());
        assertEquals(1, batch.sourcesForShard(1).size());
    }

    @Test
    void rejectsMissingDuplicateAndInvalidSelections() {
        assertThrows(IndexException.class,
                () -> load(new long[] { 2L }, new long[] { 0L, 0L }));
        assertThrows(IllegalArgumentException.class,
                () -> load(new long[] { 0L, 0L }, new long[] { 0L, 0L }));
        assertThrows(IllegalArgumentException.class,
                () -> load(new long[] { 0L }, new long[] { 0L }));
        assertThrows(IllegalArgumentException.class,
                () -> load(new long[0], new long[] { 0L, 0L }));
    }

    @Test
    void rejectsResultBeforeSubmissionOrWithWrongIdentity() {
        final SenkuL0Batch batch = load(new long[] { 0L },
                new long[] { 5L, 6L });
        final SenkuRunManifest manifest = new SenkuRunManifest(0, 0L);

        assertThrows(IndexException.class,
                () -> batch.accept(new SenkuCompletedRun(0, 0, 5L,
                        manifest)));
        assertEquals(0, batch.takeNextShard().orElseThrow());
        assertThrows(IndexException.class,
                () -> batch.accept(new SenkuCompletedRun(0, 1, 5L,
                        manifest)));
        assertThrows(IndexException.class,
                () -> batch.accept(new SenkuCompletedRun(0, 0, 7L,
                        manifest)));
    }

    private SenkuL0Batch load(final long[] flushIds, final long[] outputIds) {
        return SenkuL0Batch.load(flushDirectory, flushIds, outputIds,
                SHARD_COUNT, 4L, DATA_BLOCK_SIZE);
    }

    private void writeFlush(final long generation,
            final Map<Integer, Long> entries) {
        new SenkuFlushWriter<>(flushDirectory, keys, values,
                key -> key, SHARD_COUNT, 1, 4L, DATA_BLOCK_SIZE)
                .write(generation, entries);
    }

    private static Map<Integer, Long> entries(final int firstKey,
            final long firstValue, final int secondKey,
            final long secondValue) {
        final Map<Integer, Long> entries = new LinkedHashMap<>();
        entries.put(firstKey, firstValue);
        entries.put(secondKey, secondValue);
        return entries;
    }
}
