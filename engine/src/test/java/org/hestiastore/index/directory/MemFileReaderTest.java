package org.hestiastore.index.directory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class MemFileReaderTest {

    @Test
    void read_returnsUnsignedByteValues() {
        try (final MemFileReader reader = new MemFileReader(
                new byte[] { 0, (byte) 0xFF })) {
            assertEquals(0, reader.read());
            assertEquals(255, reader.read());
            assertEquals(-1, reader.read());
        }
    }

    @Test
    void read_readsFromByteSequenceView() {
        try (final MemFileReader reader = new MemFileReader(
                ByteSequences.viewOf(new byte[] { 9, 1, 2, 3 }, 1, 4))) {
            byte[] out = new byte[2];
            assertEquals(2, reader.read(out));
            assertEquals(1, out[0]);
            assertEquals(2, out[1]);
            assertEquals(3, reader.read());
            assertEquals(-1, reader.read());
        }
    }

    @Test
    void read_withOffset_readsIntoRequestedRange() {
        try (final MemFileReader reader = new MemFileReader(
                new byte[] { 10, 11, 12 })) {
            final byte[] out = new byte[] { 1, 1, 1, 1, 1 };
            assertEquals(2, reader.read(out, 2, 2));
            assertArrayEquals(new byte[] { 1, 1, 10, 11, 1 }, out);
            assertEquals(1, reader.read(out, 4, 1));
            assertArrayEquals(new byte[] { 1, 1, 10, 11, 12 }, out);
            assertEquals(-1, reader.read(out, 0, 1));
        }
    }
}
