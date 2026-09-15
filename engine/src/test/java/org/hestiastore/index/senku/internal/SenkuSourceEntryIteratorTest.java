package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryWriterImpl;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuSourceEntryIteratorTest {

    @Test
    void rejectsDescendingPrefixKeysInsidePage() {
        final SingleChunkEntryWriterImpl<Integer, Long> page = new SingleChunkEntryWriterImpl<>(
                keys, values);
        page.put(2, 20L);
        page.put(3, 30L);
        final byte[] encoded = page.closeSequence().toByteArrayCopy();
        encoded[encoded.length - Long.BYTES - 1] = 1;
        final LargeFileWriterTx writer = new LargeFile(directory,
                DATA_BLOCK_SIZE, 2, 0).openWriterTx();
        writer.appendPage(ByteSequences.wrap(encoded), 2);
        final int partCount = writer.commit();

        try (SenkuSourceEntryIterator<Integer, Long> source = iterator(
                partCount, 2, true)) {
            assertThrows(IndexException.class, source::next);
            assertFalse(source.hasNext());
        }
    }

    @Test
    void rejectsDescendingKeysAcrossPagesAndParts() {
        for (final long entriesPerPart : new long[] { 1, 2 }) {
            directory = new MemDirectory();
            final int partCount = write(entriesPerPart,
                    List.of(entry(2, 20L)), List.of(entry(1, 10L)));
            try (SenkuSourceEntryIterator<Integer, Long> source = iterator(
                    partCount, 2, true)) {
                assertThrows(IndexException.class, source::next);
                assertFalse(source.hasNext());
            }
        }
    }

    @Test
    void preservesDuplicateKeysAcrossPagesAndPartsForMergeReduction() {
        for (final long entriesPerPart : new long[] { 1, 2 }) {
            directory = new MemDirectory();
            final int partCount = write(entriesPerPart,
                    List.of(entry(2, 20L)), List.of(entry(2, 10L)));

            assertEquals(List.of(entry(2, 20L), entry(2, 10L)),
                    readAll(iterator(partCount, 2, true)));
        }
    }

    @Test
    void rejectsWholeMissingValueAndTrailingPartialKeyHeader() {
        final SingleChunkEntryWriterImpl<Integer, Long> page = new SingleChunkEntryWriterImpl<>(
                keys, values);
        page.put(1, 10L);
        final ByteSequence encoded = page.closeSequence();
        for (final ByteSequence invalid : List.of(
                encoded.slice(0, encoded.length() - Long.BYTES),
                ByteSequences.padToLength(encoded, encoded.length() + 1))) {
            directory = new MemDirectory();
            final LargeFileWriterTx writer = new LargeFile(directory,
                    DATA_BLOCK_SIZE, 1, 0).openWriterTx();
            writer.appendPage(invalid, 1);
            final int partCount = writer.commit();

            assertThrows(IndexException.class,
                    () -> iterator(partCount, 1, true));
        }
    }

    @Test
    void zeroByteNullValueRemainsAValidDecodedValue() {
        final TypeDescriptorNull nullValues = new TypeDescriptorNull();
        final SingleChunkEntryWriterImpl<Integer, NullValue> page = new SingleChunkEntryWriterImpl<>(
                keys, nullValues);
        page.put(1, NullValue.NULL);
        final LargeFileWriterTx writer = new LargeFile(directory,
                DATA_BLOCK_SIZE, 1, 0).openWriterTx();
        writer.appendPage(page.closeSequence(), 1);
        final LargeFile file = new LargeFile(directory, DATA_BLOCK_SIZE, 1,
                writer.commit());

        try (SenkuSourceEntryIterator<Integer, NullValue> source = new SenkuSourceEntryIterator<>(
                file.openReader(), keys, nullValues, 1, true)) {
            assertEquals(Entry.of(1, NullValue.NULL), source.next());
            assertFalse(source.hasNext());
        }
    }

    private TypeDescriptorInteger keys;
    private TypeDescriptorLong values;
    private MemDirectory directory;

    @BeforeEach
    void setUp() {
        keys = new TypeDescriptorInteger();
        values = new TypeDescriptorLong();
        directory = new MemDirectory();
    }

    @Test
    void emptySourceIsAValidEmptyRun() {
        final SenkuSourceEntryIterator<Integer, Long> iterator = iterator(0, 0L,
                true);

        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
        iterator.close();
    }

    @Test
    void readsExactCountAcrossPagesAndParts() {
        final int partCount = write(2L,
                List.of(entry(1, 10L), entry(2, 20L)),
                List.of(entry(3, 30L), entry(4, 40L)),
                List.of(entry(5, 50L)));
        final SenkuSourceEntryIterator<Integer, Long> iterator = iterator(
                partCount, 5L, true);

        assertEquals(List.of(entry(1, 10L), entry(2, 20L), entry(3, 30L),
                entry(4, 40L), entry(5, 50L)), readAll(iterator));
        assertFalse(iterator.hasNext());
    }

    @Test
    void flushRangeStopsAtPageBoundaryBeforeNextShard() {
        final int partCount = write(10L,
                List.of(entry(1, 10L), entry(2, 20L)),
                List.of(entry(100, 100L)));
        final SenkuSourceEntryIterator<Integer, Long> iterator = iterator(
                partCount, 2L, false);

        assertEquals(List.of(entry(1, 10L), entry(2, 20L)),
                readAll(iterator));
    }

    @Test
    void rejectsEarlyEndAndExtraEntryInCurrentPage() {
        final int partCount = write(10L,
                List.of(entry(1, 10L), entry(2, 20L)));

        assertThrows(IndexException.class,
                () -> readAll(iterator(partCount, 3L, true)));
        assertThrows(IndexException.class,
                () -> readAll(iterator(partCount, 1L, false)));
    }

    @Test
    void completeRunRejectsAnExtraPage() {
        final int partCount = write(10L, List.of(entry(1, 10L)),
                List.of(entry(2, 20L)));

        assertThrows(IndexException.class,
                () -> readAll(iterator(partCount, 1L, true)));
    }

    @Test
    void constructorRejectsInvalidInputs() {
        final LargeFileReader reader = new LargeFile(directory, DATA_BLOCK_SIZE,
                1L, 0).openReader();

        assertThrows(IllegalArgumentException.class,
                () -> new SenkuSourceEntryIterator<Integer, Long>(null, keys,
                        values, 0L, true));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuSourceEntryIterator<>(reader, null, values, 0L,
                        true));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuSourceEntryIterator<>(reader, keys, null, 0L,
                        true));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuSourceEntryIterator<>(reader, keys, values, -1L,
                        true));
        reader.close();
    }

    private SenkuSourceEntryIterator<Integer, Long> iterator(
            final int partCount, final long recordCount,
            final boolean requireSourceEof) {
        final LargeFile file = new LargeFile(directory, DATA_BLOCK_SIZE, 10L,
                partCount);
        return new SenkuSourceEntryIterator<>(file.openReader(), keys, values,
                recordCount, requireSourceEof);
    }

    @SafeVarargs
    private final int write(final long maxEntriesPerPart,
            final List<Entry<Integer, Long>>... pages) {
        final LargeFileWriterTx writer = new LargeFile(directory,
                DATA_BLOCK_SIZE, maxEntriesPerPart, 0).openWriterTx();
        for (final List<Entry<Integer, Long>> entries : pages) {
            final SingleChunkEntryWriterImpl<Integer, Long> page =
                    new SingleChunkEntryWriterImpl<>(keys, values);
            entries.forEach(page::put);
            writer.appendPage(page.closeSequence(), entries.size());
        }
        return writer.commit();
    }

    private static List<Entry<Integer, Long>> readAll(
            final SenkuSourceEntryIterator<Integer, Long> iterator) {
        final List<Entry<Integer, Long>> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        iterator.close();
        return result;
    }

    private static Entry<Integer, Long> entry(final int key,
            final long value) {
        return Entry.of(key, value);
    }
}
