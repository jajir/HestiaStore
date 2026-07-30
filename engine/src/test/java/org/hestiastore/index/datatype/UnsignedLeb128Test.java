package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class UnsignedLeb128Test {

    @ParameterizedTest
    @CsvSource({ "0, 1", "127, 1", "128, 2", "16383, 2", "16384, 3",
            "2097151, 3", "2097152, 4", "268435455, 4",
            "2147483647, 5" })
    void writeAndRead_roundTripsBoundaryValues(final int value,
            final int expectedBytes) {
        final ByteArrayWriter writer = new ByteArrayWriter();

        final int written = UnsignedLeb128.write(writer, value);
        final byte[] encoded = writer.toByteArray();
        final int decoded = UnsignedLeb128
                .read(new MemFileReader(encoded));

        assertEquals(expectedBytes, written);
        assertEquals(expectedBytes, encoded.length);
        assertEquals(value, decoded);
    }

    @Test
    void write_usesCanonicalLittleEndianBase128Order() {
        final ByteArrayWriter writer = new ByteArrayWriter();

        UnsignedLeb128.write(writer, 16384);

        assertArrayEquals(
                new byte[] { (byte) 0x80, (byte) 0x80, (byte) 0x01 },
                writer.toByteArray());
    }

    @Test
    void write_rejectsNegativeValue() {
        final ByteArrayWriter writer = new ByteArrayWriter();

        assertThrows(IllegalArgumentException.class,
                () -> UnsignedLeb128.write(writer, -1));
    }

    @Test
    void write_rejectsNullWriter() {
        assertThrows(IllegalArgumentException.class,
                () -> UnsignedLeb128.write(null, 0));
    }

    @Test
    void read_returnsNegativeOneOnEofBeforePrefix() {
        final int value = UnsignedLeb128
                .read(new MemFileReader(new byte[0]));

        assertEquals(-1, value);
    }

    @Test
    void read_rejectsTruncatedPrefix() {
        final MemFileReader reader = new MemFileReader(
                new byte[] { (byte) 0x80 });

        final IndexException error = assertThrows(IndexException.class,
                () -> UnsignedLeb128.read(reader));

        assertEquals("Truncated unsigned LEB128 length prefix.",
                error.getMessage());
    }

    @Test
    void read_rejectsNonCanonicalPrefix() {
        final MemFileReader reader = new MemFileReader(
                new byte[] { (byte) 0x80, 0 });

        final IndexException error = assertThrows(IndexException.class,
                () -> UnsignedLeb128.read(reader));

        assertEquals("Non-canonical unsigned LEB128 length prefix.",
                error.getMessage());
    }

    @Test
    void read_rejectsIntegerOverflow() {
        final byte[] prefix = new byte[] { (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, 0x08 };
        final MemFileReader reader = new MemFileReader(prefix);

        final IndexException error = assertThrows(IndexException.class,
                () -> UnsignedLeb128.read(reader));

        assertEquals(
                "Unsigned LEB128 length exceeds the supported integer range.",
                error.getMessage());
    }

    @Test
    void read_rejectsPrefixLongerThanFiveBytes() {
        final byte[] prefix = new byte[] { (byte) 0x80, (byte) 0x80,
                (byte) 0x80, (byte) 0x80, (byte) 0x80, 0 };
        final MemFileReader reader = new MemFileReader(prefix);

        final IndexException error = assertThrows(IndexException.class,
                () -> UnsignedLeb128.read(reader));

        assertEquals("Unsigned LEB128 length prefix exceeds five bytes.",
                error.getMessage());
    }

    @Test
    void read_rejectsNullReader() {
        assertThrows(IllegalArgumentException.class,
                () -> UnsignedLeb128.read(null));
    }
}
