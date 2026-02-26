package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class TypeEncoderTest {

    @Test
    void toByteArray_rejectsNegativeDeclaredLength() {
        final TypeEncoder<String> encoder = new TypeEncoder<String>() {
            @Override
            public int bytesLength(final String value) {
                return -1;
            }

            @Override
            public int toBytes(final String value, final byte[] destination) {
                throw new IllegalStateException("must not be called");
            }
        };

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> TestEncoding.toByteArray(encoder, "abc"));
        assertTrue(error.getMessage().contains("greater than or equal to 0"));
    }

    @Test
    void toByteArray_propagatesNonBufferExceptions() {
        final TypeEncoder<String> encoder = new TypeEncoder<String>() {
            @Override
            public int bytesLength(final String value) {
                return 3;
            }

            @Override
            public int toBytes(final String value, final byte[] destination) {
                throw new IllegalArgumentException("bad value");
            }
        };

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> TestEncoding.toByteArray(encoder, "abc"));
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
                () -> TestEncoding.toByteArray(encoder, "abcde"));
        assertTrue(error.getMessage().contains("declared"),
                "Expected mismatch message");
    }
}
