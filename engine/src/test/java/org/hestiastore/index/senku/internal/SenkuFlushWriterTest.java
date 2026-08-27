package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.hestiastore.index.senku.internal.SenkuFlushTestSupport.readShard;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuFlushWriterTest {

    private MemDirectory flushDirectory;

    @BeforeEach
    void setUp() {
        flushDirectory = new MemDirectory();
    }

    @Test
    void writePartitionsSortsPagesAndPublishesManifestLast() {
        final Map<Integer, Long> entries = new HashMap<>();
        entries.put(7, 70L);
        entries.put(0, 0L);
        entries.put(4, 40L);
        entries.put(3, 30L);
        entries.put(1, 10L);

        newWriter(value -> value, 4, 2, 3L).write(0L,
                List.of(Map.of(), entries, Map.of()));

        assertEquals(List.of(Entry.of(0, 0L), Entry.of(4, 40L)),
                readShard(flushDirectory, 0L, 0, 4, 3L));
        assertEquals(List.of(Entry.of(1, 10L)),
                readShard(flushDirectory, 0L, 1, 4, 3L));
        assertEquals(List.of(), readShard(flushDirectory, 0L, 2, 4, 3L));
        assertEquals(List.of(Entry.of(3, 30L), Entry.of(7, 70L)),
                readShard(flushDirectory, 0L, 3, 4, 3L));

        final Directory generation = flushDirectory
                .openSubDirectory("flush-00000");
        assertEquals(Set.of("part-00000.chunk", "part-00001.chunk",
                "shard-index.dat", "manifest.properties"),
                generation.getFileNames().collect(Collectors.toSet()));
        assertEquals(2,
                SenkuMetadataCodec.readFlushPartCount(generation));
    }

    @Test
    void writeUsesFloorModForNegativeAndMinimumHashes() {
        final Map<Integer, Long> entries = Map.of(1, 10L, 2, 20L);
        newWriter(key -> key == 1 ? -1 : Integer.MIN_VALUE, 3, 10, 10L)
                .write(0L, List.of(Map.of(), entries));

        assertEquals(List.of(), readShard(flushDirectory, 0L, 0, 3, 10L));
        assertEquals(List.of(Entry.of(2, 20L)),
                readShard(flushDirectory, 0L, 1, 3, 10L));
        assertEquals(List.of(Entry.of(1, 10L)),
                readShard(flushDirectory, 0L, 2, 3, 10L));
    }

    @Test
    void writeRotatesAtEveryEntryWhenConfigured() {
        newWriter(value -> 0, 1, 1, 1L).write(0L,
                List.of(Map.of(1, 10L), Map.of(),
                        Map.of(2, 20L, 3, 30L)));

        final Directory generation = flushDirectory
                .openSubDirectory("flush-00000");
        assertEquals(3,
                SenkuMetadataCodec.readFlushPartCount(generation));
    }

    @Test
    void writeRejectsEmptyMapAndExistingGeneration() {
        final SenkuFlushWriter<Integer, Long> writer = newWriter(value -> 0, 1,
                1, 1L);
        assertThrows(IllegalArgumentException.class,
                () -> writer.write(0L, List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> writer.write(0L, List.of(Map.of(), Map.of())));

        writer.write(0L, List.of(Map.of(), Map.of(1, 1L)));
        assertThrows(IndexException.class,
                () -> writer.write(0L, List.of(Map.of(2, 2L))));
    }

    @Test
    void writeConsumesMultipleDetachedStripeMaps() {
        final List<Map<Integer, Long>> entries = List.of(
                Map.of(7, 70L, 1, 10L), Map.of(),
                Map.of(4, 40L, 2, 20L));

        newWriter(value -> value, 2, 10, 10L).write(0L, entries);

        assertEquals(List.of(Entry.of(2, 20L), Entry.of(4, 40L)),
                readShard(flushDirectory, 0L, 0, 2, 10L));
        assertEquals(List.of(Entry.of(1, 10L), Entry.of(7, 70L)),
                readShard(flushDirectory, 0L, 1, 2, 10L));
    }

    @Test
    void writeSortsLargeShardRanges() {
        final int entryCount = 8_192;
        final int shardCount = 8;
        final Map<Integer, Long> entries = new HashMap<>();
        for (int key = entryCount - 1; key >= 0; key--) {
            entries.put(key, (long) key);
        }

        newWriter(value -> value, shardCount, 2_048, entryCount).write(0L,
                List.of(entries));

        for (int shardId = 0; shardId < shardCount; shardId++) {
            final List<Entry<Integer, Long>> expected = new ArrayList<>();
            for (int key = shardId; key < entryCount; key += shardCount) {
                expected.add(Entry.of(key, (long) key));
            }
            assertEquals(expected, readShard(flushDirectory, 0L, shardId,
                    shardCount, entryCount));
        }
    }

    private SenkuFlushWriter<Integer, Long> newWriter(
            final ToIntFunction<Integer> hash,
            final int shardCount, final int maxKeysPerPage,
            final long maxEntriesPerPart) {
        return new SenkuFlushWriter<>(flushDirectory,
                new TypeDescriptorInteger(), new TypeDescriptorLong(), hash,
                shardCount, maxKeysPerPage, maxEntriesPerPart,
                DATA_BLOCK_SIZE);
    }
}
