package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class LongKeyPageWriterTest {
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
