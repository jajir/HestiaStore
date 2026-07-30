package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;

class VarIntLengthReaderTest {

    @Test
    void read_returnsNullOnEofBeforePrefix() {
        final VarIntLengthReader<String> reader = createReader();

        final String value = reader.read(new MemFileReader(new byte[0]));

        assertNull(value);
    }

    @Test
    void read_decodesEmptyPayload() {
        final VarIntLengthReader<String> reader = createReader();

        final String value = reader.read(new MemFileReader(new byte[] { 0 }));

        assertEquals("", value);
    }

    @Test
    void read_decodesTwoByteLengthPrefix() {
        final VarIntLengthReader<String> reader = createReader();
        final byte[] payload = "a".repeat(128)
                .getBytes(StandardCharsets.UTF_8);
        final byte[] frame = new byte[2 + payload.length];
        frame[0] = (byte) 0x80;
        frame[1] = 0x01;
        System.arraycopy(payload, 0, frame, 2, payload.length);

        final String value = reader.read(new MemFileReader(frame));

        assertEquals("a".repeat(128), value);
    }

    @Test
    void read_rejectsTruncatedPayload() {
        final VarIntLengthReader<String> reader = createReader();
        final MemFileReader fileReader = new MemFileReader(
                new byte[] { 3, 'a', 'b' });

        final IndexException error = assertThrows(IndexException.class,
                () -> reader.read(fileReader));

        assertEquals(
                "Expected '3' bytes but reached EOF after '2' bytes.",
                error.getMessage());
    }

    @Test
    void read_rejectsNonCanonicalLengthPrefix() {
        final VarIntLengthReader<String> reader = createReader();
        final MemFileReader fileReader = new MemFileReader(
                new byte[] { (byte) 0x80, 0 });

        final IndexException error = assertThrows(IndexException.class,
                () -> reader.read(fileReader));

        assertEquals("Non-canonical unsigned LEB128 length prefix.",
                error.getMessage());
    }

    @Test
    void read_rejectsFrameLengthThatWouldOverflowWrittenByteCount() {
        final VarIntLengthReader<String> reader = createReader();
        final byte[] maximumIntegerPrefix = new byte[] { (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07 };
        final MemFileReader fileReader = new MemFileReader(maximumIntegerPrefix);

        final IndexException error = assertThrows(IndexException.class,
                () -> reader.read(fileReader));

        assertEquals(
                "Encoded payload length '2147483647' exceeds maximum '2147483642'.",
                error.getMessage());
    }

    @Test
    void constructorRejectsNullDecoder() {
        assertThrows(IllegalArgumentException.class,
                () -> new VarIntLengthReader<String>(null));
    }

    private static VarIntLengthReader<String> createReader() {
        return new VarIntLengthReader<>(Utf8StringCodec.INSTANCE);
    }
}
