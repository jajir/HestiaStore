package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkstore.Compression;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SenkuLongSourceCursorTest {

    private final TypeDescriptorLong longs = new TypeDescriptorLong();

    @Test
    void retainsEncodedRanksAcrossPagesWhileLogicalAccessorStillReturnsKeys() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                new long[] { 3 }, 0);
        final var format = new SenkuStorageFormat(codec, Compression.zstd(3));
        final var directory = new MemDirectory();
        final var manifest = new SenkuRunWriter<>(directory, longs, longs, 1,
                1L, DATA_BLOCK_SIZE, () -> true, format)
                .write(new EntryIteratorList<>(
                        List.of(Entry.of(3L, 7L), Entry.of(12L, 8L))));
        final var source = SenkuMergeSource.run(new LargeFile(directory,
                DATA_BLOCK_SIZE, 1, manifest.partCount()),
                manifest.recordCount());
        try (var cursor = source.openLongs(0, true, codec)) {
            assertEquals(0, cursor.encodedKey());
            assertEquals(3, cursor.key());
            assertEquals(7, cursor.value());
            assertTrue(codec.hasSameEncoding(cursor.keyCodec()));
            cursor.advance();
            assertEquals(1, cursor.encodedKey());
            assertEquals(12, cursor.key());
            assertEquals(8, cursor.value());
            cursor.advance();
            assertFalse(cursor.hasCurrent());
            assertThrows(IllegalStateException.class, cursor::encodedKey);
            assertThrows(IllegalStateException.class, cursor::keyCodec);
        }
    }

    @Test
    void encodedCursorRejectsOutOfDomainRankWithoutUnrankingIt() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                new long[] { 3 }, 0);
        final var format = new SenkuStorageFormat(codec, Compression.zstd(3));
        final var directory = new MemDirectory();
        final var writer = new LargeFile(directory, DATA_BLOCK_SIZE, 1L, 0,
                format).openWriterTx();
        writer.appendPage(
                ByteSequences.wrap(new byte[] { 0, 0, 0, 0, 0, 0, 0, 2 }), 1);
        final int partCount = writer.commit();
        final var source = SenkuMergeSource.run(
                new LargeFile(directory, DATA_BLOCK_SIZE, 1L, partCount), 1L);
        assertThrows(IndexException.class,
                () -> source.openLongs(0, false, codec));
    }

    @Test
    void decodesSignedLongPagesAndClosesAtExactBoundary() {
        final MemDirectory directory = new MemDirectory();
        final SenkuRunManifest manifest = new SenkuRunWriter<>(directory, longs,
                longs, 2, 4L, DATA_BLOCK_SIZE)
                .write(new EntryIteratorList<>(List.of(
                        Entry.of(Long.MIN_VALUE, 1L), Entry.of(-1L, 2L),
                        Entry.of(0L, 3L), Entry.of(Long.MAX_VALUE, 4L))));
        final SenkuMergeSource source = SenkuMergeSource
                .run(new LargeFile(directory, DATA_BLOCK_SIZE, 4L,
                        manifest.partCount()), manifest.recordCount());
        final SenkuLongSourceCursor cursor = source.openLongs(7, true);

        assertCurrent(cursor, 7, Long.MIN_VALUE, 1L);
        cursor.advance();
        assertCurrent(cursor, 7, -1L, 2L);
        cursor.advance();
        assertCurrent(cursor, 7, 0L, 3L);
        cursor.advance();
        assertCurrent(cursor, 7, Long.MAX_VALUE, 4L);
        cursor.advance();

        assertFalse(cursor.hasCurrent());
    }

    @Test
    void rejectsTruncatedPrimitiveValue() {
        final MemDirectory directory = new MemDirectory();
        final LargeFileWriterTx writer = new LargeFile(directory,
                DATA_BLOCK_SIZE, 1L, 0).openWriterTx();
        writer.appendPage(ByteSequences
                .wrap(new byte[] { 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, 0 }), 1);
        final int partCount = writer.commit();
        final SenkuMergeSource source = SenkuMergeSource.run(
                new LargeFile(directory, DATA_BLOCK_SIZE, 1L, partCount), 1L);

        assertThrows(IndexException.class, () -> source.openLongs(0, true));
    }

    private static void assertCurrent(final SenkuLongSourceCursor cursor,
            final int ordinal, final long key, final long value) {
        assertEquals(ordinal, cursor.ordinal());
        assertEquals(key, cursor.key());
        assertEquals(value, cursor.value());
    }
}
