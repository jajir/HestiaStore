package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SenkuIndexReferenceModelIT {

    @ParameterizedTest(name = "shards={0}, memory={1}, page={2}, part={3}, fanIn={4}, threads={5}, queue={6}, operations={7}")
    @CsvSource({
            "1, 1, 1, 1, 2, 1, 1, 0",
            "1, 1, 1, 1, 2, 1, 1, 1",
            "2, 2, 1, 1, 2, 1, 1, 3",
            "3, 5, 2, 3, 3, 2, 1, 50",
            "7, 11, 3, 5, 4, 4, 4, 200"
    })
    void seededBoundaryMatrixMatchesIndependentModel(final int shardCount,
            final int maxInMemoryEntries, final int maxKeysPerPage,
            final int maxEntriesPerPart, final int mergeFanIn,
            final int maintenanceThreads, final int maintenanceQueueSize,
            final int operationCount) {
        final long seed = 0x5E4B_2026L;
        assertConfiguration(seed, shardCount, maxInMemoryEntries,
                maxKeysPerPage, maxEntriesPerPart, mergeFanIn,
                maintenanceThreads, maintenanceQueueSize, operationCount);
    }

    private static void assertConfiguration(final long seed,
            final int shardCount, final int maxInMemoryEntries,
            final int maxKeysPerPage, final int maxEntriesPerPart,
            final int mergeFanIn, final int maintenanceThreads,
            final int maintenanceQueueSize, final int operationCount) {
        final Random random = new Random(seed + shardCount);
        final Map<Integer, Long> expected = new TreeMap<>();
        final MemDirectory directory = new MemDirectory();
        final SenkuMergeFunctionRegistry<Integer, Long> functions =
                new SenkuMergeFunctionRegistry<>();
        functions.register((key, first, second) -> first + second);
        final SenkuWriting<Integer, Long> writing = SenkuIndex
                .builder(directory, new TypeDescriptorInteger(),
                        new TypeDescriptorLong(), functions)
                .shardHashFunction(key -> Integer.rotateLeft(key, 7))
                .shardCount(shardCount)
                .maxInMemoryEntries(maxInMemoryEntries)
                .maxKeysPerPage(maxKeysPerPage).mergeFanIn(mergeFanIn)
                .maintenanceThreads(maintenanceThreads)
                .maintenanceQueueSize(maintenanceQueueSize)
                .diskIoBufferSize(1_024)
                .maxEntriesPerPart(maxEntriesPerPart).create();
        for (int operation = 0; operation < operationCount; operation++) {
            final int key = random.nextInt(40) - 20;
            final long value = random.nextInt(1_000);
            writing.put(key, value);
            expected.merge(key, value, Long::sum);
        }

        final SenkuReady<Integer, Long> ready = writing.finishWriting();
        final List<Entry<Integer, Long>> actual;
        try (Stream<Entry<Integer, Long>> stream = ready.openStream()) {
            actual = stream.toList();
        }
        ready.close();

        final List<Entry<Integer, Long>> expectedEntries = new ArrayList<>();
        expected.forEach((key, value) -> expectedEntries.add(Entry.of(key,
                value)));
        assertEquals(expectedEntries, actual, "seed=" + seed + ", shards="
                + shardCount + ", memory=" + maxInMemoryEntries + ", page="
                + maxKeysPerPage + ", part=" + maxEntriesPerPart + ", fanIn="
                + mergeFanIn + ", threads=" + maintenanceThreads + ", queue="
                + maintenanceQueueSize + ", operations=" + operationCount);
    }
}
