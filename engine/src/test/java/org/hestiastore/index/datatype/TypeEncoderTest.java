package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TypeEncoderTest {

    @Test
    void toByteArray_propagatesEncodeExceptions() {
        final TypeEncoder<String> encoder = new TypeEncoder<String>() {
            @Override
            public EncodedBytes encode(final String value,
                    final byte[] reusableBuffer) {
                throw new IllegalArgumentException("bad value");
            }
        };

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> TestEncoding.toByteArray(encoder, "abc"));
        assertEquals("bad value", error.getMessage());
    }

    @Test
    void toByteArray_trimsReturnedBufferToEncodedLength() {
        final TypeEncoder<String> encoder = new TypeEncoder<String>() {
            @Override
            public EncodedBytes encode(final String value,
                    final byte[] reusableBuffer) {
                return new EncodedBytes(new byte[] { 'a', 'b', 'c', 'x' }, 3);
            }
        };

        assertArrayEquals(new byte[] { 'a', 'b', 'c' },
                TestEncoding.toByteArray(encoder, "abc"));
    }

    @Test
    void encode_reusesProvidedBufferWhenItIsLargeEnough() {
        final TypeEncoder<String> encoder = new TypeEncoder<String>() {
            @Override
            public EncodedBytes encode(final String value,
                    final byte[] reusableBuffer) {
                reusableBuffer[0] = 'a';
                reusableBuffer[1] = 'b';
                reusableBuffer[2] = 'c';
                return new EncodedBytes(reusableBuffer, 3);
            }
        };

        final byte[] reusable = new byte[16];
        final EncodedBytes encoded = encoder.encode("abc", reusable);

        assertSame(reusable, encoded.getBytes());
        assertEquals(3, encoded.getLength());
        assertArrayEquals(new byte[] { 'a', 'b', 'c' },
                new byte[] { encoded.getBytes()[0], encoded.getBytes()[1],
                        encoded.getBytes()[2] });
    }

    @Test
    void encode_allocatesLargerBufferWhenReusableIsTooSmall() {
        final TypeEncoder<String> encoder = new TypeEncoder<String>() {
            @Override
            public EncodedBytes encode(final String value,
                    final byte[] reusableBuffer) {
                final byte[] output = new byte[] { 'a', 'b', 'c', 'd' };
                return new EncodedBytes(output, output.length);
            }
        };

        final byte[] reusable = new byte[2];
        final EncodedBytes encoded = encoder.encode("abcd", reusable);

        assertEquals(4, encoded.getLength());
        assertArrayEquals(new byte[] { 'a', 'b', 'c', 'd' },
                new byte[] { encoded.getBytes()[0], encoded.getBytes()[1],
                        encoded.getBytes()[2], encoded.getBytes()[3] });
    }
}
