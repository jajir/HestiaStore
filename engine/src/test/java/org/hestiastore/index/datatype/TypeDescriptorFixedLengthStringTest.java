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

        final byte[] bytes = TestEncoding.toByteArray(descriptor.getTypeEncoder(),
                value);
        final String decoded = descriptor.getTypeDecoder().decode(bytes);

        assertEquals(4, descriptor.getTypeEncoder().bytesLength(value));
        assertArrayEquals(value.getBytes(StandardCharsets.ISO_8859_1), bytes);
        assertEquals(value, decoded);
    }

    @Test
    void encoderRejectsWrongStringLength() {
        final TypeDescriptorFixedLengthString descriptor = new TypeDescriptorFixedLengthString(
                3);

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> descriptor.getTypeEncoder().bytesLength("ABCD"));
        assertTrue(error.getMessage().contains("String length"));
    }

    @Test
    void encoderRejectsTooSmallDestination() {
        final TypeDescriptorFixedLengthString descriptor = new TypeDescriptorFixedLengthString(
                4);

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, () -> descriptor.getTypeEncoder()
                        .toBytes("ABCD", new byte[3]));
        assertTrue(error.getMessage().contains("Destination buffer too small"));
    }

    @Test
    void decoderRejectsWrongByteArrayLength() {
        final TypeDescriptorFixedLengthString descriptor = new TypeDescriptorFixedLengthString(
                4);

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> descriptor.getTypeDecoder().decode(new byte[3]));
        assertTrue(error.getMessage().contains("Byte array length should be"));
    }

    @Test
    void encoderRejectsSurrogatePairWhenFixedLengthCannotBeSatisfied() {
        final TypeDescriptorFixedLengthString descriptor = new TypeDescriptorFixedLengthString(
                2);

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, () -> descriptor.getTypeEncoder()
                        .toBytes("🙂", new byte[2]));
        assertTrue(error.getMessage().contains("should be encoded"));
    }
}
