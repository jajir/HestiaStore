package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import java.util.Map;
import java.util.stream.LongStream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkstore.Compression;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SenkuFixedWeightCodecIT {

    @TempDir
    private Path temporaryDirectory;

    @Test
    void filesystemReopenReconstructsDomainAfterMultipleFlushesAndLevels()
            throws IOException {
        final var directory = new FsDirectory(temporaryDirectory.toFile());
        final var functions = new SenkuMergeFunctionRegistry<Long, Long>();
        functions.register((key, left, right) -> left + right);
        final var writing = SenkuIndex
                .builder(directory, new TypeDescriptorLong(),
                        new TypeDescriptorLong(), functions)
                .keyPageCodec(KeyPageCodecs.longFixedWeightDeltaVarint(8, 4,
                        new long[] { 3, 12 }, 1))
                .compression(Compression.zstd(3))
                .shardHashFunction(key -> Long.hashCode(key)).shardCount(3)
                .maxInMemoryEntries(3).maxKeysPerPage(2).maxEntriesPerPart(4)
                .mergeFanIn(2).maintenanceThreads(2).maintenanceQueueSize(4)
                .diskIoBufferSize(1024).create();
        final long[] keys = LongStream.range(0, 256)
                .filter(key -> Long.bitCount(key) == 4
                        && (Long.bitCount(key & 3) & 1) == 1
                        && (Long.bitCount(key & 12) & 1) == 0)
                .toArray();
        final Map<Long, Long> expected = new TreeMap<>();
        for (int repeat = 0; repeat < 3; repeat++) {
            for (int i = keys.length - 1; i >= 0; i--) {
                writing.put(keys[i], (long) repeat + 1);
                expected.merge(keys[i], (long) repeat + 1, Long::sum);
            }
        }
        final List<Entry<Long, Long>> entries = expected.entrySet().stream()
                .map(entry -> Entry.of(entry.getKey(), entry.getValue()))
                .toList();
        try (var ready = writing.finishWriting();
                var stream = ready.openStream()) {
            assertEquals(entries, stream.toList());
        }
        try (var reopened = SenkuIndex.open(
                new FsDirectory(temporaryDirectory.toFile()),
                new TypeDescriptorLong(), new TypeDescriptorLong(), 1024);
                var stream = reopened.openStream()) {
            assertEquals(entries, stream.toList());
        }
        final Path metadata = temporaryDirectory.resolve("format.properties");
        final String format = Files.readString(metadata,
                StandardCharsets.ISO_8859_1);
        assertTrue(format.contains("keyPageCodec=4"));
        Files.writeString(metadata, format.replace("fixedWeightBitCount=8",
                "fixedWeightBitCount=64"), StandardCharsets.ISO_8859_1);
        assertThrows(IndexException.class, () -> SenkuIndex.open(directory,
                new TypeDescriptorLong(), new TypeDescriptorLong(), 1024));
    }

    @Test
    void genericValueMergeAlsoRetainsLogicalKeysAndDomain() {
        final var directory = new MemDirectory();
        final var functions = new SenkuMergeFunctionRegistry<Long, Integer>();
        functions.register((key, left, right) -> left + right);
        final var writing = SenkuIndex
                .builder(directory, new TypeDescriptorLong(),
                        new TypeDescriptorInteger(), functions)
                .keyPageCodec(KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                        new long[0], 0))
                .shardCount(2).shardHashFunction(key -> Long.hashCode(key))
                .maxInMemoryEntries(1).maxKeysPerPage(1).maxEntriesPerPart(2)
                .mergeFanIn(2).maintenanceThreads(2).maintenanceQueueSize(4)
                .diskIoBufferSize(1024).create();
        for (final long key : new long[] { 12, 3, 5, 12, 3 }) {
            writing.put(key, 1);
        }
        try (var ready = writing.finishWriting();
                var stream = ready.openStream()) {
            assertEquals(
                    List.of(Entry.of(3L, 2), Entry.of(5L, 1), Entry.of(12L, 2)),
                    stream.toList());
        }
    }

    @Test
    void emptyRankedIndexReopensWithoutAnyPageOrApplicationRegistration() {
        final var directory = new MemDirectory();
        final var functions = new SenkuMergeFunctionRegistry<Long, NullValue>();
        functions.register((key, left, right) -> NullValue.NULL);
        final var writing = SenkuIndex
                .builder(directory, new TypeDescriptorLong(),
                        new TypeDescriptorNull(), functions)
                .keyPageCodec(KeyPageCodecs.longFixedWeightDeltaVarint(49, 0,
                        new long[0], 0))
                .shardCount(1).shardHashFunction(key -> Long.hashCode(key))
                .maxInMemoryEntries(4).maxKeysPerPage(2).maxEntriesPerPart(4)
                .mergeFanIn(2).maintenanceThreads(1).maintenanceQueueSize(2)
                .diskIoBufferSize(1024).create();
        writing.finishWriting().close();
        try (var ready = SenkuIndex.open(directory, new TypeDescriptorLong(),
                new TypeDescriptorNull(), 1024);
                var stream = ready.openStream()) {
            assertEquals(0, stream.count());
        }
    }
}
