package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuRunWriterTest {

    @Test
    void inputCloseFailurePreventsPublishingCompletedParts() {
        final IndexException closeFailure = new IndexException("Input close failed");
        final EntryIteratorList<Integer, Long> input = new EntryIteratorList<>(
                List.of(entry(1, 10L))) {
            @Override
            protected void doClose() {
                throw closeFailure;
            }
        };
        final SenkuRunWriter<Integer, Long> runWriter = writer(1, 1);

        assertSame(closeFailure,
                assertThrows(IndexException.class, () -> runWriter.write(input)));
        assertTrue(input.wasClosed());
        assertTrue(directory.isFileExists(SenkuFileNames.partFile(0)));
        assertFalse(directory.isFileExists(SenkuFileNames.MANIFEST_FILE));
    }

    @Test
    void repeatedWriteClosesInputAndPreservesPublishedManifest() {
        final SenkuRunWriter<Integer, Long> runWriter = writer(1, 1);
        runWriter.write(new EntryIteratorList<>(List.of(entry(1, 10L))));
        final EntryIteratorList<Integer, Long> nextInput = new EntryIteratorList<>(
                List.of(entry(2, 20L)));

        assertThrows(IndexException.class, () -> runWriter.write(nextInput));
        assertTrue(nextInput.wasClosed());
        assertEquals(List.of(entry(1, 10L)),
                read(SenkuMetadataCodec.readRunManifest(directory)));
    }

    @Test
    void rejectsDuplicateAndDescendingKeysAtNewPageBoundary() {
        for (final int nextKey : new int[] { 1, 2 }) {
            directory = new MemDirectory();
            final SenkuRunWriter<Integer, Long> runWriter = writer(1, 1);
            final EntryIteratorList<Integer, Long> input = new EntryIteratorList<>(
                    List.of(entry(2, 20L), entry(nextKey, 10L)));

            assertThrows(IndexException.class, () -> runWriter.write(input));
            assertTrue(input.wasClosed());
            assertFalse(directory.isFileExists(SenkuFileNames.MANIFEST_FILE));
        }
    }

    @Test
    void longOutputPublishesBoundedWeightedSummaryWithItsManifest() {
        final TypeDescriptorLong longs = new TypeDescriptorLong();
        final SenkuRunManifest manifest = new SenkuRunWriter<>(directory, longs,
                longs, 2, 4L, DATA_BLOCK_SIZE)
                .write(new EntryIteratorList<>(List.of(Entry.of(1L, 10L),
                        Entry.of(4L, 40L), Entry.of(8L, 80L))));
        assertEquals(3, manifest.longKeySummary().orElseThrow().recordCount());
        assertEquals(3, manifest.longKeySummary().orElseThrow().keys().length);
        assertEquals(3, SenkuMetadataCodec.readRunManifest(directory)
                .longKeySummary().orElseThrow().recordCount());
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
    void writesEmptyManifestOnlyRun() {
        final EntryIteratorList<Integer, Long> input = new EntryIteratorList<>(
                List.of());

        final SenkuRunManifest manifest = writer(2, 4L).write(input);

        assertEquals(0, manifest.partCount());
        assertEquals(0L, manifest.recordCount());
        assertTrue(input.wasClosed());
        assertEquals(List.of(SenkuFileNames.MANIFEST_FILE),
                directory.getFileNames().sorted().toList());
    }

    @Test
    void writesPagesAndPartsThenPublishesExactManifest() {
        final List<Entry<Integer, Long>> expected = List.of(entry(1, 10L),
                entry(2, 20L), entry(3, 30L), entry(4, 40L), entry(5, 50L));
        final EntryIteratorList<Integer, Long> input = new EntryIteratorList<>(
                expected);

        final SenkuRunManifest manifest = writer(2, 4L).write(input);

        assertEquals(2, manifest.partCount());
        assertEquals(5L, manifest.recordCount());
        final SenkuRunManifest persisted = SenkuMetadataCodec
                .readRunManifest(directory);
        assertEquals(manifest.partCount(), persisted.partCount());
        assertEquals(manifest.recordCount(), persisted.recordCount());
        assertEquals(expected, read(manifest));
    }

    @Test
    void writeFailureClosesInputAndDoesNotPublishManifest() {
        directory.touch(SenkuFileNames.partFile(0));
        final EntryIteratorList<Integer, Long> input = new EntryIteratorList<>(
                List.of(entry(1, 1L)));

        assertThrows(IndexException.class, () -> writer(1, 1L).write(input));

        assertTrue(input.wasClosed());
        assertFalse(directory.isFileExists(SenkuFileNames.MANIFEST_FILE));
    }

    @Test
    void publicationGuardLeavesCommittedPartsWithoutManifest() {
        final EntryIteratorList<Integer, Long> input = new EntryIteratorList<>(
                List.of(entry(1, 1L)));
        final SenkuRunWriter<Integer, Long> writer = new SenkuRunWriter<>(
                directory, keys, values, 1, 1L, DATA_BLOCK_SIZE, () -> false);

        assertThrows(IndexException.class, () -> writer.write(input));

        assertTrue(input.wasClosed());
        assertTrue(directory.isFileExists(SenkuFileNames.partFile(0)));
        assertFalse(directory.isFileExists(SenkuFileNames.MANIFEST_FILE));
    }

    @Test
    void validatesDependenciesAndLimits() {
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunWriter<Integer, Long>(null, keys, values, 1,
                        1L, DATA_BLOCK_SIZE));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunWriter<>(directory, null, values, 1, 1L,
                        DATA_BLOCK_SIZE));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunWriter<>(directory, keys, null, 1, 1L,
                        DATA_BLOCK_SIZE));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunWriter<>(directory, keys, values, 0, 1L,
                        DATA_BLOCK_SIZE));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunWriter<>(directory, keys, values, 2, 1L,
                        DATA_BLOCK_SIZE));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuRunWriter<>(directory, keys, values, 1, 1L,
                        null));
        assertThrows(IllegalArgumentException.class,
                () -> writer(1, 1L).write(null));
    }

    private SenkuRunWriter<Integer, Long> writer(final int maxKeysPerPage,
            final long maxEntriesPerPart) {
        return new SenkuRunWriter<>(directory, keys, values, maxKeysPerPage,
                maxEntriesPerPart, DATA_BLOCK_SIZE);
    }

    private List<Entry<Integer, Long>> read(final SenkuRunManifest manifest) {
        final LargeFile file = new LargeFile(directory, DATA_BLOCK_SIZE, 4L,
                manifest.partCount());
        final SenkuSourceEntryIterator<Integer, Long> iterator = new SenkuSourceEntryIterator<>(
                file.openReader(), keys, values, manifest.recordCount(), true);
        final List<Entry<Integer, Long>> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        iterator.close();
        return entries;
    }

    private static Entry<Integer, Long> entry(final int key, final long value) {
        return Entry.of(key, value);
    }
}
