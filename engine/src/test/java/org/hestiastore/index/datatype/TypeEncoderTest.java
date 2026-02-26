package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class TypeEncoderTest {

    @Test
    void toByteArray_supportsUnknownLength() {
        final String value = "x".repeat(100);
        final UnknownLengthEncoder encoder = new UnknownLengthEncoder();

        final byte[] out = TypeEncoder.toByteArray(encoder, value);

        assertArrayEquals(value.getBytes(StandardCharsets.ISO_8859_1), out);
        assertTrue(encoder.getInvocationCount() >= 2,
                "Expected at least one retry when initial 64-byte buffer is too small");
    }

    @Test
    void toByteArray_propagatesNonBufferExceptions() {
        final TypeEncoder<String> encoder = new TypeEncoder<String>() {
            @Override
            public int bytesLength(final String value) {
                return -1;
            }

            @Override
            public int toBytes(final String value, final byte[] destination) {
                throw new IllegalArgumentException("bad value");
            }
        };

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> TypeEncoder.toByteArray(encoder, "abc"));
        assertEquals("bad value", error.getMessage());
    }

    @Test
    void toByteArray_throwsWhenDeclaredLengthDoesNotMatchWritten() {
        final TypeEncoder<String> encoder = new TypeEncoder<String>() {
            @Override
            public int bytesLength(final String value) {
                return 5;
            }

            @Override
            public int toBytes(final String value, final byte[] destination) {
                final byte[] bytes = value.getBytes(StandardCharsets.ISO_8859_1);
                System.arraycopy(bytes, 0, destination, 0, 3);
                return 3;
            }
        };

        final IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> TypeEncoder.toByteArray(encoder, "abcde"));
        assertTrue(error.getMessage().contains("declared"),
                "Expected mismatch message");
    }

    private static final class UnknownLengthEncoder
            implements TypeEncoder<String> {

        private int invocationCount = 0;

        @Override
        public int bytesLength(final String value) {
            return -1;
        }

        @Override
        public int toBytes(final String value, final byte[] destination) {
            invocationCount++;
            final byte[] bytes = value.getBytes(StandardCharsets.ISO_8859_1);
            if (destination.length < bytes.length) {
                throw new IllegalArgumentException(
                        "Destination buffer too small.");
            }
            System.arraycopy(bytes, 0, destination, 0, bytes.length);
            return bytes.length;
        }

        int getInvocationCount() {
            return invocationCount;
        }
    }
}
