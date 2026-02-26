package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;

class VarLengthReaderTest {

    private static final TypeEncoder<Integer> INTEGER_ENCODER = new TypeDescriptorInteger()
            .getTypeEncoder();

    @Test
    void read_returnsNullOnEofBeforeLengthPrefix() {
        final VarLengthReader<String> reader = createStringReader();

        final String value = reader.read(new MemFileReader(new byte[0]));

        assertNull(value);
    }

    @Test
    void read_throwsOnTruncatedLengthPrefix() {
        final VarLengthReader<String> reader = createStringReader();

        final IndexException error = assertThrows(IndexException.class,
                () -> reader.read(new MemFileReader(new byte[] { 0, 0, 0 })));

        assertTrue(error.getMessage().contains("Expected '4' bytes"));
    }

    @Test
    void read_returnsNullForNegativeLengthSentinel() {
        final VarLengthReader<String> reader = createStringReader();
        final byte[] encodedLength = TestEncoding.toByteArray(INTEGER_ENCODER,
                -1);

        final String value = reader.read(new MemFileReader(encodedLength));

        assertNull(value);
    }

    @Test
    void read_throwsOnTruncatedPayload() {
        final VarLengthReader<String> reader = createStringReader();
        final byte[] encodedLength = TestEncoding.toByteArray(INTEGER_ENCODER,
                3);
        final byte[] payload = new byte[] { 'a', 'b' };

        final IndexException error = assertThrows(IndexException.class,
                () -> reader.read(new MemFileReader(concat(encodedLength,
                        payload))));

        assertTrue(error.getMessage().contains("Expected '3' bytes"));
    }

    @Test
    void read_decodesValueWhenLengthAndPayloadArePresent() {
        final VarLengthReader<String> reader = createStringReader();
        final byte[] encodedLength = TestEncoding.toByteArray(INTEGER_ENCODER,
                3);
        final byte[] payload = new byte[] { 'a', 'b', 'c' };

        final String value = reader.read(new MemFileReader(concat(encodedLength,
                payload)));

        assertEquals("abc", value);
    }

    private static VarLengthReader<String> createStringReader() {
        return new VarLengthReader<>(new TypeDescriptorString().getTypeDecoder());
    }

    private static byte[] concat(final byte[] first, final byte[] second) {
        final byte[] out = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, out, first.length, second.length);
        return out;
    }
}
