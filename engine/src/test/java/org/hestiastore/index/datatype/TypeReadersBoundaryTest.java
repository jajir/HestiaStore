package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;

class TypeReadersBoundaryTest {

    @Test
    void byteReader_returnsNullAtEof() {
        final TypeReader<Byte> reader = new TypeDescriptorByte()
                .getTypeReader();

        final Byte value = reader.read(new MemFileReader(new byte[0]));

        assertNull(value);
    }

    @Test
    void byteReader_reads0xFFAsValidByteValue() {
        final TypeReader<Byte> reader = new TypeDescriptorByte()
                .getTypeReader();

        final Byte value = reader
                .read(new MemFileReader(new byte[] { (byte) 0xFF }));

        assertEquals(Byte.valueOf((byte) -1), value);
    }

    @Test
    void integerReader_throwsOnTruncatedRecord() {
        assertTruncatedReadThrows(new TypeDescriptorInteger(), 3);
    }

    @Test
    void integerReader_returnsNullAtEof() {
        assertNullWhenNoDataAreAvailable(new TypeDescriptorInteger());
    }

    @Test
    void longReader_throwsOnTruncatedRecord() {
        assertTruncatedReadThrows(new TypeDescriptorLong(), 7);
    }

    @Test
    void longReader_returnsNullAtEof() {
        assertNullWhenNoDataAreAvailable(new TypeDescriptorLong());
    }

    @Test
    void floatReader_throwsOnTruncatedRecord() {
        assertTruncatedReadThrows(new TypeDescriptorFloat(), 3);
    }

    @Test
    void floatReader_returnsNullAtEof() {
        assertNullWhenNoDataAreAvailable(new TypeDescriptorFloat());
    }

    @Test
    void doubleReader_throwsOnTruncatedRecord() {
        assertTruncatedReadThrows(new TypeDescriptorDouble(), 7);
    }

    @Test
    void doubleReader_returnsNullAtEof() {
        assertNullWhenNoDataAreAvailable(new TypeDescriptorDouble());
    }

    @Test
    void fixedLengthStringReader_returnsNullAtEof() {
        final TypeReader<String> reader = new TypeDescriptorFixedLengthString(4)
                .getTypeReader();

        final String value = reader.read(new MemFileReader(new byte[0]));

        assertNull(value);
    }

    @Test
    void fixedLengthStringReader_throwsOnTruncatedRecord() {
        assertTruncatedReadThrows(new TypeDescriptorFixedLengthString(4), 3);
    }

    private static <T> void assertTruncatedReadThrows(
            final TypeDescriptor<T> descriptor, final int bytesAvailable) {
        final byte[] data = new byte[bytesAvailable];
        final TypeReader<T> reader = descriptor.getTypeReader();
        final MemFileReader fileReader = new MemFileReader(data);

        final IndexException error = assertThrows(IndexException.class,
                () -> reader.read(fileReader));

        assertTrue(error.getMessage().contains("Expected"));
        assertTrue(error.getMessage().contains("EOF"));
    }

    private static <T> void assertNullWhenNoDataAreAvailable(
            final TypeDescriptor<T> descriptor) {
        final TypeReader<T> reader = descriptor.getTypeReader();

        final T value = reader.read(new MemFileReader(new byte[0]));

        assertNull(value);
    }
}
