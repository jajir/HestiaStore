package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryWriterImpl;
import org.hestiastore.index.chunkstore.Compression;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.senku.SenkuMergeFunctions;
import org.junit.jupiter.api.Test;

class SenkuLongMergeWriterTest {
    @Test
    void rankedSetMergeMatchesIndependentOraclePagesPartsAndSamples() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(8, 4,
                new long[0], 0);
        final SenkuStorageFormat format = new SenkuStorageFormat(codec,
                Compression.zstd(3));
        final TypeDescriptorNull nulls = new TypeDescriptorNull();
        final List<Entry<Long, NullValue>> expected = new ArrayList<>();
        final List<SenkuMergeSource> sources = new ArrayList<>();
        for (long rank = 0; rank < 70; rank++) {
            expected.add(Entry.of(codec.decodeLongKey(rank), NullValue.NULL));
        }
        for (int ordinal = 0; ordinal < 3; ordinal++) {
            final List<Entry<Long, NullValue>> entries = new ArrayList<>();
            for (int rank = 0; rank < expected.size(); rank++) {
                if (rank % 3 != ordinal) {
                    entries.add(expected.get(rank));
                    if (rank % 10 == 0) {
                        entries.add(expected.get(rank));
                    }
                }
            }
            sources.add(
                    sourcePages(new MemDirectory(), nulls, format, entries));
        }
        final MemDirectory actual = new MemDirectory();
        final MemDirectory oracle = new MemDirectory();
        final SenkuRunManifest actualManifest = new SenkuLongMergeWriter<>(
                sources, actual, longs, nulls, SenkuMergeFunctions.longSet(), 7,
                10, DATA_BLOCK_SIZE, () -> true, format).write();
        final SenkuRunManifest oracleManifest = new SenkuRunWriter<>(oracle,
                longs, nulls, 7, 10, DATA_BLOCK_SIZE, () -> true, format)
                .write(new EntryIteratorList<>(expected));
        assertEquals(oracleManifest.partCount(), actualManifest.partCount());
        assertEquals(oracleManifest.recordCount(),
                actualManifest.recordCount());
        assertEquals(oracle.getFileNames().sorted().toList(),
                actual.getFileNames().sorted().toList());
        for (int part = 0; part < actualManifest.partCount(); part++) {
            final String name = SenkuFileNames.partFile(part);
            assertArrayEquals(oracle.getFileSequence(name).toByteArray(),
                    actual.getFileSequence(name).toByteArray());
        }
        assertArrayEquals(oracleManifest.longKeySummary().orElseThrow().keys(),
                actualManifest.longKeySummary().orElseThrow().keys());
        assertArrayEquals(
                oracleManifest.longKeySummary().orElseThrow().weights(),
                actualManifest.longKeySummary().orElseThrow().weights());
    }

    @Test
    void repeatedRankedKeysWithinAndAcrossSourcesKeepNoncommutativeOrder() {
        final SenkuStorageFormat format = rankedFormat();
        final SenkuMergeTrackingDirectory first = new SenkuMergeTrackingDirectory();
        final SenkuMergeTrackingDirectory second = new SenkuMergeTrackingDirectory();
        final SenkuMergeTrackingDirectory third = new SenkuMergeTrackingDirectory();
        final List<SenkuMergeSource> sources = List.of(
                sourcePages(first, longs, format,
                        List.of(Entry.of(3L, 1L), Entry.of(3L, 2L),
                                Entry.of(12L, 7L))),
                sourcePages(second, longs, format,
                        List.of(Entry.of(3L, 3L), Entry.of(3L, 4L),
                                Entry.of(12L, 8L))),
                sourcePages(third, longs, format, List.of(Entry.of(3L, 5L),
                        Entry.of(5L, 6L), Entry.of(12L, 9L))));
        final List<Long> callbacks = new ArrayList<>();
        final MemDirectory output = new MemDirectory();
        final SenkuRunManifest manifest = new SenkuLongMergeWriter<>(sources,
                output, longs, longs, (key, left, right) -> {
                    callbacks.add(key);
                    return left * 10L + right;
                }, 2, 4, DATA_BLOCK_SIZE, () -> true, format).write();
        assertEquals(
                List.of(Entry.of(3L, 12345L), Entry.of(5L, 6L),
                        Entry.of(12L, 789L)),
                readWithFormat(output, manifest, longs, format));
        assertEquals(List.of(3L, 3L, 3L, 3L, 12L, 12L), callbacks);
        assertArrayEquals(new long[] { 3L, 5L, 12L },
                manifest.longKeySummary().orElseThrow().keys());
        assertReadersClosed(first, second, third);
    }

    @Test
    void genericNullReducerRetainsEveryDuplicateCallAndLogicalRankedKey() {
        final SenkuStorageFormat format = rankedFormat();
        final TypeDescriptorNull nulls = new TypeDescriptorNull();
        final List<SenkuMergeSource> sources = List.of(
                sourcePages(new MemDirectory(), nulls, format,
                        List.of(Entry.of(3L, NullValue.NULL),
                                Entry.of(3L, NullValue.NULL),
                                Entry.of(12L, NullValue.NULL))),
                sourcePages(new MemDirectory(), nulls, format,
                        List.of(Entry.of(3L, NullValue.NULL),
                                Entry.of(12L, NullValue.NULL),
                                Entry.of(12L, NullValue.NULL))));
        final List<Long> callbacks = new ArrayList<>();
        final List<NullValue> accumulated = new ArrayList<>();
        final MemDirectory output = new MemDirectory();
        final SenkuRunManifest manifest = new SenkuLongMergeWriter<>(sources,
                output, longs, nulls, (key, left, right) -> {
                    callbacks.add(key);
                    accumulated.add(left);
                    return NullValue.TOMBSTONE;
                }, 1, 4, DATA_BLOCK_SIZE, () -> true, format).write();
        assertEquals(List.of(3L, 3L, 12L, 12L), callbacks);
        assertEquals(List.of(NullValue.NULL, NullValue.TOMBSTONE,
                NullValue.NULL, NullValue.TOMBSTONE), accumulated);
        assertEquals(
                List.of(Entry.of(3L, NullValue.NULL),
                        Entry.of(12L, NullValue.NULL)),
                readWithFormat(output, manifest, nulls, format));
    }

    @Test
    void randomizedSignedSourcesAndDuplicatesMatchSourceOrderedOracle() {
        final Random random = new Random(417L);
        final SenkuStorageFormat format = SenkuStorageFormat.createDefault();
        for (final int count : new int[] { 0, 1, 2, 3, 8, 31 }) {
            final List<SenkuMergeSource> sources = new ArrayList<>();
            final Map<Long, Long> expected = new TreeMap<>();
            for (int ordinal = 0; ordinal < count; ordinal++) {
                final List<Entry<Long, Long>> entries = new ArrayList<>();
                for (int index = random.nextInt(15); index > 0; index--) {
                    final long key = index == 2 ? Long.MIN_VALUE
                            : index == 1 ? Long.MAX_VALUE
                                    : random.nextInt(9) - 4L;
                    entries.add(Entry.of(key, random.nextLong()));
                }
                entries.sort((first, second) -> Long.compare(first.getKey(),
                        second.getKey()));
                for (final Entry<Long, Long> entry : entries) {
                    expected.merge(entry.getKey(), entry.getValue(),
                            (left, right) -> left * 31L + right);
                }
                sources.add(sourcePages(new MemDirectory(), longs, format,
                        entries));
            }
            final MemDirectory output = new MemDirectory();
            final SenkuRunManifest manifest = new SenkuLongMergeWriter<>(
                    sources, output, longs, longs,
                    (key, left, right) -> left * 31L + right, 3, 4,
                    DATA_BLOCK_SIZE, () -> true, format).write();
            assertEquals(
                    expected.entrySet().stream()
                            .map(entry -> Entry.of(entry.getKey(),
                                    entry.getValue()))
                            .toList(),
                    readWithFormat(output, manifest, longs, format));
            assertEquals(expected.size(), manifest.recordCount());
        }
    }

    @Test
    void reducerFailureClosesSelectedAndUnselectedInputsAndKeepsPrimary() {
        final SenkuStorageFormat format = rankedFormat();
        final SenkuMergeTrackingDirectory first = new SenkuMergeTrackingDirectory();
        final SenkuMergeTrackingDirectory second = new SenkuMergeTrackingDirectory();
        final SenkuMergeTrackingDirectory third = new SenkuMergeTrackingDirectory();
        final List<SenkuMergeSource> sources = List.of(
                sourcePages(first, longs, format,
                        List.of(Entry.of(3L, 1L), Entry.of(3L, 2L),
                                Entry.of(12L, 3L))),
                sourcePages(second, longs, format,
                        List.of(Entry.of(3L, 4L), Entry.of(12L, 5L))),
                sourcePages(third, longs, format,
                        List.of(Entry.of(5L, 6L), Entry.of(12L, 7L))));
        first.closeFailure = new IndexException("First close failed");
        second.closeFailure = new IndexException("Second close failed");
        third.closeFailure = new IndexException("Third close failed");
        final IndexException failure = new IndexException("Reducer failed");
        final MemDirectory output = new MemDirectory();
        final SenkuLongMergeWriter<Long> writer = new SenkuLongMergeWriter<>(
                sources, output, longs, longs, (key, left, right) -> {
                    throw failure;
                }, 1, 4, DATA_BLOCK_SIZE, () -> true, format);
        assertSame(failure, assertThrows(IndexException.class, writer::write));
        assertEquals(List.of(first.closeFailure, second.closeFailure,
                third.closeFailure), List.of(failure.getSuppressed()));
        assertReadersClosed(first, second, third);
        assertFalse(output.isFileExists(SenkuFileNames.MANIFEST_FILE));
    }

    @Test
    void sourceOpenAndPrematureEofFailuresCloseEveryOpenedReader() {
        final SenkuStorageFormat format = SenkuStorageFormat.createDefault();
        for (final boolean duringOpen : new boolean[] { true, false }) {
            final SenkuMergeTrackingDirectory first = new SenkuMergeTrackingDirectory();
            final SenkuMergeTrackingDirectory second = new SenkuMergeTrackingDirectory();
            final SenkuMergeSource healthy = sourcePages(first, longs, format,
                    List.of(Entry.of(-1L, 1L), Entry.of(5L, 2L)));
            sourcePages(second, longs, format,
                    List.of(Entry.of(0L, 3L), Entry.of(6L, 4L)));
            final SenkuMergeSource malformed = SenkuMergeSource.run(
                    new LargeFile(second, DATA_BLOCK_SIZE, 1000, 1),
                    duringOpen ? 0 : 3);
            final MemDirectory output = new MemDirectory();
            final SenkuLongMergeWriter<Long> writer = new SenkuLongMergeWriter<>(
                    List.of(healthy, malformed), output, longs, longs,
                    (key, left, right) -> left + right, 1, 4, DATA_BLOCK_SIZE,
                    () -> true, format);
            assertThrows(IndexException.class, writer::write);
            assertReadersClosed(first, second);
            assertFalse(output.isFileExists(SenkuFileNames.MANIFEST_FILE));
        }
    }

    @Test
    void codecAndOutputFailuresDoNotLeakSelectedOrQueuedInputs() {
        final SenkuStorageFormat format = rankedFormat();
        for (final boolean wrongCodec : new boolean[] { true, false }) {
            final SenkuMergeTrackingDirectory first = new SenkuMergeTrackingDirectory();
            final SenkuMergeTrackingDirectory second = new SenkuMergeTrackingDirectory();
            final List<SenkuMergeSource> sources = List.of(
                    sourcePages(first, longs, format,
                            List.of(Entry.of(3L, 1L), Entry.of(12L, 2L))),
                    sourcePages(second, longs,
                            wrongCodec ? SenkuStorageFormat.createDefault()
                                    : format,
                            List.of(Entry.of(5L, 3L), Entry.of(12L, 4L))));
            final MemDirectory output = new MemDirectory();
            if (!wrongCodec) {
                output.touch(SenkuFileNames.partFile(0));
            }
            final SenkuLongMergeWriter<Long> writer = new SenkuLongMergeWriter<>(
                    sources, output, longs, longs,
                    (key, left, right) -> left + right, 1, 4, DATA_BLOCK_SIZE,
                    () -> true, format);
            assertThrows(IndexException.class, writer::write);
            assertReadersClosed(first, second);
            assertFalse(output.isFileExists(SenkuFileNames.MANIFEST_FILE));
        }
    }

    @Test
    void arbitraryRankedNullReducerStillReceivesLogicalKeyAndItsFailureWins() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                new long[0], 0);
        final var format = new SenkuStorageFormat(codec, Compression.zstd(3));
        final var first = rankedNullSource(format, 3L, 12L);
        final var second = rankedNullSource(format, 5L, 12L);
        final var output = new MemDirectory();
        final var expectedFailure = new IndexException("logical duplicate");
        final List<Long> duplicateKeys = new ArrayList<>();
        final var writer = new SenkuLongMergeWriter<>(List.of(first, second),
                output, longs, new TypeDescriptorNull(), (key, left, right) -> {
                    duplicateKeys.add(key);
                    throw expectedFailure;
                }, 1, 2, DATA_BLOCK_SIZE, () -> true, format);
        assertSame(expectedFailure,
                assertThrows(IndexException.class, writer::write));
        assertEquals(List.of(12L), duplicateKeys);
        assertFalse(output.isFileExists(SenkuFileNames.MANIFEST_FILE));
    }

    @Test
    void explicitRankedLongSetDeduplicatesWithEncodedKeysAndLogicalReadback() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                new long[0], 0);
        final var format = new SenkuStorageFormat(codec, Compression.zstd(3));
        final var output = new MemDirectory();
        final var manifest = new SenkuLongMergeWriter<>(
                List.of(rankedNullSource(format, 3, 6, 12),
                        rankedNullSource(format, 5, 6, 10, 12)),
                output, longs, new TypeDescriptorNull(),
                SenkuMergeFunctions.longSet(), 2, 3, DATA_BLOCK_SIZE,
                () -> true, format).write();
        final var result = SenkuMergeSource.run(
                new LargeFile(output, DATA_BLOCK_SIZE, 3, manifest.partCount()),
                manifest.recordCount());
        final List<Long> keys = new ArrayList<>();
        try (var reader = result.open(longs, new TypeDescriptorNull(), codec)) {
            while (reader.hasNext()) {
                keys.add(reader.next().getKey());
            }
        }
        assertEquals(List.of(3L, 5L, 6L, 10L, 12L), keys);
        assertEquals(5, manifest.recordCount());
    }

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

    private SenkuMergeSource rankedNullSource(final SenkuStorageFormat format,
            final long... keys) {
        final var directory = new MemDirectory();
        final List<Entry<Long, NullValue>> entries = new ArrayList<>();
        for (final long key : keys) {
            entries.add(Entry.of(key, NullValue.NULL));
        }
        final var manifest = new SenkuRunWriter<>(directory, longs,
                new TypeDescriptorNull(), 1, 2, DATA_BLOCK_SIZE, () -> true,
                format).write(new EntryIteratorList<>(entries));
        return SenkuMergeSource.run(new LargeFile(directory, DATA_BLOCK_SIZE, 2,
                manifest.partCount()), manifest.recordCount());
    }

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

    private SenkuStorageFormat rankedFormat() {
        return new SenkuStorageFormat(
                KeyPageCodecs.longFixedWeightDeltaVarint(4, 2, new long[0], 0),
                Compression.zstd(3));
    }

    /** One record per page permits valid repeated source keys across pages. */
    private <V> SenkuMergeSource sourcePages(final Directory directory,
            final TypeDescriptor<V> values, final SenkuStorageFormat format,
            final List<Entry<Long, V>> entries) {
        final LargeFileWriterTx writer = new LargeFile(directory,
                DATA_BLOCK_SIZE, 1000, 0, format).openWriterTx();
        for (final Entry<Long, V> entry : entries) {
            final SingleChunkEntryWriterImpl<Long, V> page = new SingleChunkEntryWriterImpl<>(
                    longs, values, 18, format.keyCodec());
            page.put(entry);
            writer.appendPage(page.closeSequence(), 1);
        }
        final int partCount = writer.commit();
        return SenkuMergeSource.run(
                new LargeFile(directory, DATA_BLOCK_SIZE, 1000, partCount),
                entries.size());
    }

    private <V> List<Entry<Long, V>> readWithFormat(final Directory directory,
            final SenkuRunManifest manifest, final TypeDescriptor<V> values,
            final SenkuStorageFormat format) {
        final SenkuMergeSource source = SenkuMergeSource
                .run(new LargeFile(directory, DATA_BLOCK_SIZE, 4,
                        manifest.partCount()), manifest.recordCount());
        final List<Entry<Long, V>> entries = new ArrayList<>();
        try (var reader = source.open(longs, values, format.keyCodec())) {
            while (reader.hasNext()) {
                entries.add(reader.next());
            }
        }
        return entries;
    }

    private static void assertReadersClosed(
            final SenkuMergeTrackingDirectory... directories) {
        for (final SenkuMergeTrackingDirectory directory : directories) {
            assertFalse(directory.readers.isEmpty());
            assertTrue(directory.readers.stream()
                    .allMatch(FileReaderSeekable::wasClosed));
        }
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
