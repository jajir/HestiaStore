package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class TypeDescriptorFixedLengthStringTest {

    @Test
    void constructorRejectsLength128() {
        assertThrows(IllegalArgumentException.class,
                () -> new TypeDescriptorFixedLengthString(128));
    }

    @Test
    void encoderAndDecoder_roundTripFixedLengthValue() {
        final TypeDescriptorFixedLengthString descriptor = new TypeDescriptorFixedLengthString(
                4);
        final String value = "ABCD";

        final EncodedBytes encoded = descriptor.getTypeEncoder().encode(value,
                new byte[0]);
        final byte[] bytes = TestEncoding.toByteArray(
                descriptor.getTypeEncoder(), value);
        final String decoded = descriptor.getTypeDecoder().decode(bytes);

        assertEquals(4, encoded.getLength());
        assertArrayEquals(value.getBytes(StandardCharsets.ISO_8859_1), bytes);
        assertEquals(value, decoded);
    }

    @Test
    void encoderRejectsWrongStringLength() {
        final TypeDescriptorFixedLengthString descriptor = new TypeDescriptorFixedLengthString(
                3);
        final TypeEncoder<String> encoder = descriptor.getTypeEncoder();

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, () -> encoder.encode("ABCD",
                        new byte[0]));
        assertTrue(error.getMessage().contains("String length"));
    }

    @Test
    void encoderResizesTooSmallReusableBuffer() {
        final TypeDescriptorFixedLengthString descriptor = new TypeDescriptorFixedLengthString(
                4);

        final EncodedBytes encoded = descriptor.getTypeEncoder().encode("ABCD",
                new byte[3]);

        assertEquals(4, encoded.getLength());
        assertArrayEquals("ABCD".getBytes(StandardCharsets.ISO_8859_1),
                new byte[] { encoded.getBytes()[0], encoded.getBytes()[1],
                        encoded.getBytes()[2], encoded.getBytes()[3] });
    }

    @Test
    void decoderRejectsWrongByteArrayLength() {
        final TypeDescriptorFixedLengthString descriptor = new TypeDescriptorFixedLengthString(
                4);
        final TypeDecoder<String> decoder = descriptor.getTypeDecoder();
        final byte[] bytes = new byte[3];

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, () -> decoder.decode(bytes));
        assertTrue(error.getMessage().contains("Byte array length should be"));
    }

    @Test
    void encoderRejectsSurrogatePairWhenFixedLengthCannotBeSatisfied() {
        final TypeDescriptorFixedLengthString descriptor = new TypeDescriptorFixedLengthString(
                2);
        final TypeEncoder<String> encoder = descriptor.getTypeEncoder();
        final byte[] reusableBuffer = new byte[2];

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> encoder.encode("🙂", reusableBuffer));
        assertTrue(error.getMessage().contains("should be encoded"));
    }
}
