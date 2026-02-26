package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;

class VarShortLengthReaderTest {

    @Test
    void read_returnsNullOnEofBeforeLength() {
        final VarShortLengthReader<String> reader = createStringReader();

        final String value = reader.read(new MemFileReader(new byte[0]));

        assertNull(value);
    }

    @Test
    void read_throwsWhenLengthIsGreaterThan127() {
        final VarShortLengthReader<String> reader = createStringReader();

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> reader.read(new MemFileReader(new byte[] { (byte) 0x80 })));

        assertEquals("Converted type is too big", error.getMessage());
    }

    @Test
    void read_throwsOnTruncatedPayload() {
        final VarShortLengthReader<String> reader = createStringReader();

        final IndexException error = assertThrows(IndexException.class,
                () -> reader.read(new MemFileReader(new byte[] { 3, 'a', 'b' })));

        assertTrue(error.getMessage().contains("Expected '3' bytes"));
    }

    @Test
    void read_decodesValueWhenLengthAndPayloadArePresent() {
        final VarShortLengthReader<String> reader = createStringReader();

        final String value = reader
                .read(new MemFileReader(new byte[] { 3, 'a', 'b', 'c' }));

        assertEquals("abc", value);
    }

    @Test
    void read_supportsZeroLengthPayload() {
        final VarShortLengthReader<String> reader = createStringReader();

        final String value = reader.read(new MemFileReader(new byte[] { 0 }));

        assertEquals("", value);
    }

    private static VarShortLengthReader<String> createStringReader() {
        return new VarShortLengthReader<>(
                new TypeDescriptorString().getTypeDecoder());
    }
}
