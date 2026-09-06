package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class LongKeyPageWriterTest {

    @Test
    void encodedRanksMatchLogicalWritesWithoutBeingRankedAgain() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                new long[0], 0);
        final var encodedWriter = new LongKeyPageWriter(codec);
        final var logicalWriter = new LongKeyPageWriter(codec);
        final var encodedBytes = new InMemoryFileWriter();
        final var logicalBytes = new InMemoryFileWriter();
        for (long rank = 0; rank < 6; rank++) {
            encodedWriter.writeEncoded(encodedBytes, rank, codec);
            logicalWriter.write(logicalBytes, codec.decodeLongKey(rank));
        }
        assertArrayEquals(logicalBytes.closeSequence().toByteArray(),
                encodedBytes.closeSequence().toByteArray());
    }

    @Test
    void encodedWritesRejectWrongDomainsAndBoundsBeforeChangingPageState() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                new long[] { 3 }, 0);
        final var otherDomain = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                new long[] { 3 }, 1);
        final var writer = new LongKeyPageWriter(codec);
        final var bytes = new InMemoryFileWriter();
        assertThrows(IllegalArgumentException.class,
                () -> writer.writeEncoded(bytes, 0, otherDomain));
        assertThrows(IndexException.class,
                () -> writer.writeEncoded(bytes, -1, codec));
        assertThrows(IndexException.class,
                () -> writer.writeEncoded(bytes, 2, codec));
        writer.writeEncoded(bytes, 0, codec);
        assertThrows(IllegalArgumentException.class,
                () -> writer.writeEncoded(bytes, 0, codec));
        writer.write(bytes, 12);
        assertArrayEquals(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 1 },
                bytes.closeSequence().toByteArray());
    }

    @Test
    void fixedWeightWritesRanksAndRejectsInvalidKeysBeforeWritingBytes() {
        final var writer = new LongKeyPageWriter(
                KeyPageCodecs.longFixedWeightDeltaVarint(4, 2, new long[0], 0));
        final var bytes = new InMemoryFileWriter();
        writer.write(bytes, 3);
        assertThrows(IndexException.class, () -> writer.write(bytes, 8));
        writer.write(bytes, 5);
        writer.write(bytes, 12);
        assertThrows(IllegalArgumentException.class,
                () -> writer.write(bytes, 12));
        assertArrayEquals(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 1, 4 },
                bytes.closeSequence().toByteArray());
    }

    @Test
    void deltaStoresFirstAbsoluteAndSmallGapsWithoutLengthHeaders() {
        final var writer = new LongKeyPageWriter(
                KeyPageCodecs.longDeltaVarint());
        final var bytes = new InMemoryFileWriter();
        for (long key : new long[] { 1000, 1003, 1010, 1011 })
            writer.write(bytes, key);
        assertArrayEquals(
                new byte[] { 0, 0, 0, 0, 0, 0, 3, (byte) 232, 3, 7, 1 },
                bytes.closeSequence().toByteArray());
    }

    @Test
    void fullUnsignedGapNeedsTenBytesAndOrderingIsValidated() {
        final var writer = new LongKeyPageWriter(
                KeyPageCodecs.longDeltaVarint());
        final var bytes = new InMemoryFileWriter();
        writer.write(bytes, Long.MIN_VALUE);
        writer.write(bytes, Long.MAX_VALUE);
        assertEquals(18, bytes.closeSequence().length());
        assertThrows(IllegalArgumentException.class,
                () -> writer.write(bytes, Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class,
                () -> writer.write(bytes, 0));
    }
}
