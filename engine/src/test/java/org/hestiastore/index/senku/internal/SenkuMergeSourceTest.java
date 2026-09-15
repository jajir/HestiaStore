package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkstore.Compression;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SenkuMergeSourceTest {
    @Test
    void rankDomainIsAvailableToBothGenericAndPrimitiveSourceReaders() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                new long[] { 3 }, 0);
        final var format = new SenkuStorageFormat(codec, Compression.zstd(3));
        final var directory = new MemDirectory();
        final var entries = List.of(Entry.of(3L, 10L), Entry.of(12L, 20L));
        final var manifest = new SenkuRunWriter<>(directory, values, values, 1,
                2, DATA_BLOCK_SIZE, () -> true, format)
                .write(new EntryIteratorList<>(entries));
        final var file = new LargeFile(directory, DATA_BLOCK_SIZE, 2,
                manifest.partCount());
        final var source = SenkuMergeSource.run(file, manifest.recordCount());
        try (var reader = source.open(values, values, codec)) {
            assertEquals(entries.get(0), reader.next());
            assertEquals(entries.get(1), reader.next());
            assertFalse(reader.hasNext());
        }
        try (var reader = source.openLongs(0, true, codec)) {
            assertEquals(3, reader.key());
            assertEquals(10, reader.value());
            reader.advance();
            assertEquals(12, reader.key());
            assertEquals(20, reader.value());
            reader.advance();
            assertFalse(reader.hasCurrent());
        }
        final var wrongCodec = KeyPageCodecs.longDeltaVarint();
        assertThrows(IndexException.class,
                () -> source.open(values, values, wrongCodec));
        assertThrows(IndexException.class,
                () -> source.openLongs(0, true, wrongCodec));
    }

    private final TypeDescriptorInteger keys = new TypeDescriptorInteger();
    private final TypeDescriptorLong values = new TypeDescriptorLong();

    @Test
    void runSourceReadsCompleteRunAndEmptyRun() {
        final MemDirectory directory = new MemDirectory();
        final List<Entry<Integer, Long>> expected = List.of(Entry.of(1, 10L),
                Entry.of(2, 20L));
        final SenkuRunManifest manifest = new SenkuRunWriter<>(directory, keys,
                values, 1, 2L, DATA_BLOCK_SIZE)
                .write(new EntryIteratorList<>(expected));
        final LargeFile file = new LargeFile(directory, DATA_BLOCK_SIZE, 2L,
                manifest.partCount());

        assertEquals(expected,
                read(SenkuMergeSource.run(file, manifest.recordCount())));

        final MemDirectory emptyDirectory = new MemDirectory();
        assertFalse(SenkuMergeSource
                .run(new LargeFile(emptyDirectory, DATA_BLOCK_SIZE, 2L, 0), 0L)
                .open(keys, values).hasNext());
    }

    @Test
    void factoriesRejectInvalidRanges() {
        final LargeFile file = new LargeFile(new MemDirectory(),
                DATA_BLOCK_SIZE, 2L, 0);

        assertThrows(IllegalArgumentException.class, () -> SenkuMergeSource
                .flush(file, LargeFilePosition.of(0, 0), 0L));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuMergeSource.flush(file, null, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuMergeSource.run(file, -1L));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuMergeSource.run(null, 0L));
    }

    private List<Entry<Integer, Long>> read(final SenkuMergeSource source) {
        final List<Entry<Integer, Long>> entries = new ArrayList<>();
        try (EntryIterator<Integer, Long> iterator = source.open(keys,
                values)) {
            while (iterator.hasNext()) {
                entries.add(iterator.next());
            }
        }
        return entries;
    }
}
