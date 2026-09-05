package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkstore.Compression;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SenkuLongMergeWriterTest {
    @Test
    void rankedInputsReduceUsingLogicalKeysAndPersistRankedOutput() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                new long[0], 0);
        final var format = new SenkuStorageFormat(codec, Compression.zstd(3));
        final var first = rankedSource(format, Entry.of(3L, 1L),
                Entry.of(12L, 2L));
        final var second = rankedSource(format, Entry.of(5L, 4L),
                Entry.of(12L, 5L));
        final var output = new MemDirectory();
        final List<Long> duplicateKeys = new ArrayList<>();
        final var manifest = new SenkuLongMergeWriter<>(List.of(first, second),
                output, longs, longs, (key, left, right) -> {
                    duplicateKeys.add(key);
                    return left + right;
                }, 1, 2, DATA_BLOCK_SIZE, () -> true, format).write();
        final var source = SenkuMergeSource.run(
                new LargeFile(output, DATA_BLOCK_SIZE, 2, manifest.partCount()),
                manifest.recordCount());
        try (var reader = source.open(longs, longs, codec)) {
            assertEquals(Entry.of(3L, 1L), reader.next());
            assertEquals(Entry.of(5L, 4L), reader.next());
            assertEquals(Entry.of(12L, 7L), reader.next());
            assertFalse(reader.hasNext());
        }
        assertEquals(List.of(12L), duplicateKeys);
    }

    @SafeVarargs
    private final SenkuMergeSource rankedSource(final SenkuStorageFormat format,
            final Entry<Long, Long>... entries) {
        final var directory = new MemDirectory();
        final var manifest = new SenkuRunWriter<>(directory, longs, longs, 1, 2,
                DATA_BLOCK_SIZE, () -> true, format)
                .write(new EntryIteratorList<>(List.of(entries)));
        return SenkuMergeSource.run(new LargeFile(directory, DATA_BLOCK_SIZE, 2,
                manifest.partCount()), manifest.recordCount());
    }

    private final TypeDescriptorLong longs = new TypeDescriptorLong();

    @Test
    void mergesWithoutBoxedIntermediateEntriesAndPreservesSourceOrder() {
        final SenkuMergeSource first = source(Entry.of(-2L, 1L),
                Entry.of(5L, 2L));
        final SenkuMergeSource second = source(Entry.of(-2L, 3L),
                Entry.of(0L, 4L), Entry.of(5L, 5L));
        final MemDirectory output = new MemDirectory();

        final SenkuRunManifest manifest = new SenkuLongMergeWriter<>(
                List.of(first, second), output, longs, longs,
                (key, left, right) -> left * 10L + right, 2, 4L,
                DATA_BLOCK_SIZE, () -> true).write();

        assertEquals(List.of(Entry.of(-2L, 13L), Entry.of(0L, 4L),
                Entry.of(5L, 25L)), read(output, manifest));
        assertEquals(3L, manifest.recordCount());
    }

    @Test
    void publishesEmptyRunAndFencesPublicationAfterPartCommit() {
        final MemDirectory empty = new MemDirectory();
        final SenkuRunManifest emptyManifest = new SenkuLongMergeWriter<>(
                List.of(), empty, longs, longs,
                (key, left, right) -> left + right, 2, 4L, DATA_BLOCK_SIZE,
                () -> true).write();

        assertEquals(0, emptyManifest.partCount());
        assertEquals(0L, emptyManifest.recordCount());

        final MemDirectory fenced = new MemDirectory();
        assertThrows(IndexException.class,
                () -> new SenkuLongMergeWriter<>(
                        List.of(source(Entry.of(1L, 1L))), fenced, longs, longs,
                        (key, left, right) -> left + right, 1, 1L,
                        DATA_BLOCK_SIZE, () -> false).write());
        assertFalse(fenced.isFileExists(SenkuFileNames.MANIFEST_FILE));
        assertEquals(List.of(SenkuFileNames.partFile(0)),
                fenced.getFileNames().toList());
    }

    @Test
    void primitiveLongKeyPathSupportsZeroByteNullValuesAndDuplicates() {
        final TypeDescriptorNull nulls = new TypeDescriptorNull();
        final SenkuMergeSource first = nullSource(-1L, 2L);
        final SenkuMergeSource second = nullSource(-1L, 0L, 3L);
        final MemDirectory output = new MemDirectory();
        final int[] duplicateCalls = { 0 };

        final SenkuRunManifest manifest = new SenkuLongMergeWriter<>(
                List.of(first, second), output, longs, nulls,
                (key, left, right) -> {
                    duplicateCalls[0]++;
                    return NullValue.NULL;
                }, 2, 4L, DATA_BLOCK_SIZE, () -> true).write();

        assertEquals(1, duplicateCalls[0]);
        assertEquals(List.of(-1L, 0L, 2L, 3L),
                readNullKeys(output, manifest, nulls));
    }

    @Test
    void mergeJobSelectsPrimitiveLongNullPath() {
        final TypeDescriptorNull nulls = new TypeDescriptorNull();
        final MemDirectory output = new MemDirectory();
        final SenkuCompletedRun completed = new SenkuMergeJob<>(
                List.of(nullSource(1L, 3L), nullSource(2L, 3L)), output, 0, 1,
                9L, longs, nulls, (key, left, right) -> NullValue.NULL, 2, 4L,
                DATA_BLOCK_SIZE).execute();

        assertEquals(9L, completed.runId());
        assertEquals(List.of(1L, 2L, 3L),
                readNullKeys(output, completed.manifest(), nulls));
    }

    @SafeVarargs
    private final SenkuMergeSource source(final Entry<Long, Long>... entries) {
        final MemDirectory directory = new MemDirectory();
        final SenkuRunManifest manifest = new SenkuRunWriter<>(directory, longs,
                longs, 2, 4L, DATA_BLOCK_SIZE)
                .write(new EntryIteratorList<>(List.of(entries)));
        return SenkuMergeSource.run(new LargeFile(directory, DATA_BLOCK_SIZE,
                4L, manifest.partCount()), manifest.recordCount());
    }

    private SenkuMergeSource nullSource(final long... keys) {
        final TypeDescriptorNull nulls = new TypeDescriptorNull();
        final List<Entry<Long, NullValue>> entries = new ArrayList<>();
        for (final long key : keys) {
            entries.add(Entry.of(key, NullValue.NULL));
        }
        final MemDirectory directory = new MemDirectory();
        final SenkuRunManifest manifest = new SenkuRunWriter<>(directory, longs,
                nulls, 2, 4L, DATA_BLOCK_SIZE)
                .write(new EntryIteratorList<>(entries));
        return SenkuMergeSource.run(new LargeFile(directory, DATA_BLOCK_SIZE,
                4L, manifest.partCount()), manifest.recordCount());
    }

    private List<Entry<Long, Long>> read(final MemDirectory directory,
            final SenkuRunManifest manifest) {
        final SenkuSourceEntryIterator<Long, Long> iterator = new SenkuSourceEntryIterator<>(
                new LargeFile(directory, DATA_BLOCK_SIZE, 4L,
                        manifest.partCount()).openReader(),
                longs, longs, manifest.recordCount(), true);
        final List<Entry<Long, Long>> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        iterator.close();
        return entries;
    }

    private List<Long> readNullKeys(final MemDirectory directory,
            final SenkuRunManifest manifest,
            final TypeDescriptorNull valueTypeDescriptor) {
        final SenkuSourceEntryIterator<Long, NullValue> iterator = new SenkuSourceEntryIterator<>(
                new LargeFile(directory, DATA_BLOCK_SIZE, 4L,
                        manifest.partCount()).openReader(),
                longs, valueTypeDescriptor, manifest.recordCount(), true);
        final List<Long> keys = new ArrayList<>();
        while (iterator.hasNext()) {
            keys.add(iterator.next().getKey());
        }
        iterator.close();
        return keys;
    }
}
