package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;

class UnsignedByteLengthReaderTest {

    @Test
    void read_returnsNullOnEofBeforePrefix() {
        final UnsignedByteLengthReader<String> reader = createReader();

        final String value = reader.read(new MemFileReader(new byte[0]));

        assertNull(value);
    }

    @Test
    void read_decodesEmptyPayload() {
        final UnsignedByteLengthReader<String> reader = createReader();

        final String value = reader
                .read(new MemFileReader(new byte[] { 0 }));

        assertEquals("", value);
    }

    @Test
    void read_treatsPrefixAsUnsignedAt255Boundary() {
        final UnsignedByteLengthReader<String> reader = createReader();
        final byte[] frame = new byte[256];
        frame[0] = (byte) 0xFF;
        Arrays.fill(frame, 1, frame.length, (byte) 'a');

        final String value = reader.read(new MemFileReader(frame));

        assertEquals("a".repeat(255), value);
    }

    @Test
    void read_rejectsTruncatedPayload() {
        final UnsignedByteLengthReader<String> reader = createReader();
        final MemFileReader fileReader = new MemFileReader(
                new byte[] { 3, 'a', 'b' });

        final IndexException error = assertThrows(IndexException.class,
                () -> reader.read(fileReader));

        assertEquals(
                "Expected '3' bytes but reached EOF after '2' bytes.",
                error.getMessage());
    }

    @Test
    void read_rejectsNullReader() {
        final UnsignedByteLengthReader<String> reader = createReader();

        assertThrows(IllegalArgumentException.class,
                () -> reader.read(null));
    }

    @Test
    void constructorRejectsNullDecoder() {
        assertThrows(IllegalArgumentException.class,
                () -> new UnsignedByteLengthReader<String>(null));
    }

    private static UnsignedByteLengthReader<String> createReader() {
        return new UnsignedByteLengthReader<>(Utf8StringCodec.INSTANCE);
    }
}
