package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SenkuIndexIT {

    @Test
    void publicApiMatchesIndependentReferenceAcrossPagesPartsAndShards() {
        final MemDirectory directory = new MemDirectory();
        final Map<Integer, Long> expected = new TreeMap<>();
        final SenkuWriting<Integer, Long> writing = writing(directory, 4, 100,
                2, 4L, 3);
        for (int value = 29; value >= 0; value--) {
            final int key = value % 17;
            writing.put(key, (long) value);
            expected.merge(key, (long) value, Long::sum);
        }

        final SenkuReady<Integer, Long> ready = writing.finishWriting();

        assertEquals(entries(expected), read(ready));
        ready.close();
        final SenkuReady<Integer, Long> reopened = SenkuIndex.open(directory,
                new TypeDescriptorInteger(), new TypeDescriptorLong(), 1_024);
        assertEquals(entries(expected), read(reopened));
        reopened.close();
    }

    @Test
    void drainBuildsMultipleLevelsFromFullAndPartialFlushGroups() {
        final MemDirectory directory = new MemDirectory();
        final SenkuWriting<Integer, Long> writing = writing(directory, 2, 1, 1,
                1L, 2);
        writing.put(3, 30L);
        writing.put(1, 10L);
        writing.put(2, 20L);

        final SenkuReady<Integer, Long> ready = writing.finishWriting();

        assertEquals(List.of(Entry.of(1, 10L), Entry.of(2, 20L),
                Entry.of(3, 30L)), read(ready));
        ready.close();
    }

    @Test
    void emptyAndAllDuplicateIndexesHaveExactStreams() {
        final MemDirectory emptyDirectory = new MemDirectory();
        final SenkuReady<Integer, Long> empty = writing(emptyDirectory, 3, 10,
                2, 4L, 2).finishWriting();
        assertEquals(List.of(), read(empty));
        empty.close();

        final MemDirectory duplicateDirectory = new MemDirectory();
        final SenkuWriting<Integer, Long> duplicates = writing(
                duplicateDirectory, 1, 10, 2, 4L, 2);
        for (int value = 1; value <= 100; value++) {
            duplicates.put(7, (long) value);
        }
        final SenkuReady<Integer, Long> duplicateReady = duplicates
                .finishWriting();
        assertEquals(List.of(Entry.of(7, 5_050L)), read(duplicateReady));
        duplicateReady.close();
    }

    @Test
    void thresholdPlusOneAndSkewedHashProduceExactReadyLayout() {
        final MemDirectory directory = new MemDirectory();
        final SenkuWriting<Integer, Long> writing = writing(directory, 8, 2, 1,
                1L, 4, key -> 0);
        writing.put(3, 30L);
        writing.put(1, 10L);
        writing.put(2, 20L);

        final SenkuReady<Integer, Long> ready = writing.finishWriting();

        assertEquals(List.of(Entry.of(1, 10L), Entry.of(2, 20L),
                Entry.of(3, 30L)), read(ready));
        assertEquals(Set.of(".lock", "ready.properties", "flush",
                "shard-00000", "shard-00001", "shard-00002",
                "shard-00003", "shard-00004", "shard-00005",
                "shard-00006", "shard-00007"),
                directory.getFileNames().collect(Collectors.toSet()));
        assertEquals(List.of(), directory.openSubDirectory("flush")
                .getFileNames().toList());
        ready.close();
        assertFalse(directory.isFileExists(".lock"));
    }

    private static SenkuWriting<Integer, Long> writing(
            final MemDirectory directory, final int shardCount,
            final int maxInMemoryEntries, final int maxKeysPerPage,
            final long maxEntriesPerPart, final int mergeFanIn) {
        return writing(directory, shardCount, maxInMemoryEntries,
                maxKeysPerPage, maxEntriesPerPart, mergeFanIn, key -> key);
    }

    private static SenkuWriting<Integer, Long> writing(
            final MemDirectory directory, final int shardCount,
            final int maxInMemoryEntries, final int maxKeysPerPage,
            final long maxEntriesPerPart, final int mergeFanIn,
            final ToIntFunction<Integer> shardHashFunction) {
        final SenkuMergeFunctionRegistry<Integer, Long> functions =
                new SenkuMergeFunctionRegistry<>();
        functions.register((key, first, second) -> first + second);
        return SenkuIndex
                .builder(directory, new TypeDescriptorInteger(),
                        new TypeDescriptorLong(), functions)
                .shardHashFunction(shardHashFunction).shardCount(shardCount)
                .maxInMemoryEntries(maxInMemoryEntries)
                .maxKeysPerPage(maxKeysPerPage).mergeFanIn(mergeFanIn)
                .maintenanceThreads(4).maintenanceQueueSize(4)
                .diskIoBufferSize(1_024)
                .maxEntriesPerPart(maxEntriesPerPart).create();
    }

    private static List<Entry<Integer, Long>> read(
            final SenkuReady<Integer, Long> ready) {
        try (Stream<Entry<Integer, Long>> stream = ready.openStream()) {
            return stream.toList();
        }
    }

    private static List<Entry<Integer, Long>> entries(
            final Map<Integer, Long> values) {
        final List<Entry<Integer, Long>> entries = new ArrayList<>();
        values.forEach((key, value) -> entries.add(Entry.of(key, value)));
        return entries;
    }
}
