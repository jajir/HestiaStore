package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.ArrayList;
import java.util.List;
import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkstore.Compression;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.senku.SenkuIndex;
import org.hestiastore.index.senku.SenkuMergeFunctionRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@Timeout(30)
class SenkuCompressionTest {
    private final TypeDescriptorLong longs = new TypeDescriptorLong();

    @ParameterizedTest
    @CsvSource({ "false,0", "false,3", "true,0", "true,1", "true,3" })
    void flushMergeRotateAndReopenPreserveLongValues(boolean delta, int level) {
        final var directory = new MemDirectory();
        final var functions = new SenkuMergeFunctionRegistry<Long, Long>();
        functions.register((key, left, right) -> left + right);
        final var codec = delta ? KeyPageCodecs.longDeltaVarint()
                : KeyPageCodecs.<Long>prefix();
        final var writing = SenkuIndex
                .builder(directory, longs, longs, functions).keyPageCodec(codec)
                .compression(level == 0 ? Compression.none()
                        : Compression.zstd(level))
                .shardHashFunction(Long::intValue).shardCount(3)
                .maxInMemoryEntries(7).maxKeysPerPage(3).maxEntriesPerPart(6)
                .mergeFanIn(2).maintenanceThreads(2).maintenanceQueueSize(4)
                .diskIoBufferSize(1024).create();
        final List<Long> keys = new ArrayList<>();
        keys.add(Long.MIN_VALUE);
        for (long k = -2; k <= 2; k++)
            keys.add(k * 127);
        keys.add(1L << 48);
        keys.add(Long.MAX_VALUE);
        for (int round = 0; round < 3; round++) {
            for (int i = keys.size() - 1; i >= 0; i--)
                writing.put(keys.get(i), 1L);
        }
        final List<Entry<Long, Long>> expected = keys.stream()
                .map(k -> Entry.of(k, 3L)).toList();
        try (var ready = writing.finishWriting();
                var stream = ready.openStream()) {
            assertEquals(expected, stream.toList());
        }
        final var format = SenkuMetadataCodec.readStorageFormat(directory);
        assertEquals(codec.getId(), format.keyCodec().getId());
        assertEquals(level, format.compression().getLevel());
        try (var ready = SenkuIndex.open(directory, longs, longs, 1024);
                var stream = ready.openStream()) {
            assertEquals(expected, stream.toList());
        }
    }

    @Test
    void boardNullValuesDeduplicateAndOldMetadataIsRejected() {
        final var directory = new MemDirectory();
        final var nulls = new TypeDescriptorNull();
        final var functions = new SenkuMergeFunctionRegistry<Long, NullValue>();
        functions.register((key, left, right) -> NullValue.NULL);
        final var writing = SenkuIndex
                .builder(directory, longs, nulls, functions)
                .keyPageCodec(KeyPageCodecs.longDeltaVarint())
                .compression(Compression.zstd(3))
                .shardHashFunction(Long::intValue).shardCount(2)
                .maxInMemoryEntries(4).maxKeysPerPage(2).maxEntriesPerPart(4)
                .mergeFanIn(2).maintenanceThreads(1).maintenanceQueueSize(2)
                .diskIoBufferSize(1024).create();
        for (int i = 0; i < 30; i++)
            writing.put((1L << 48) + (i % 9), NullValue.NULL);
        try (var ready = writing.finishWriting();
                var stream = ready.openStream()) {
            assertEquals(9, stream.count());
        }
        directory.deleteFile(SenkuFileNames.FORMAT_FILE);
        SenkuMetadataCodec.publishStorageFormat(directory,
                SenkuStorageFormat.createDefault());
        try (var ready = SenkuIndex.open(directory, longs, nulls, 1024)) {
            assertThrows(IndexException.class, ready::openStream);
        }
        directory.deleteFile(SenkuFileNames.FORMAT_FILE);
        assertThrows(IndexException.class,
                () -> SenkuIndex.open(directory, longs, nulls, 1024));
    }
}
